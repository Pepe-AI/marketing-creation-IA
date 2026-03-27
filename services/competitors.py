"""
services/competitors.py — Etapa 3.2: Análisis de competidores.

Flujo completo:
A) Resolver nombre → URL de Facebook (SerpAPI + cache)
B) Scraping con Apify (Facebook Ads Library Scraper)
C) Análisis Gemini Vision por anuncio
D) Síntesis por competidor + síntesis cruzada
"""

import os
import json
import asyncio
import logging
from typing import Optional

import httpx

from ai.provider import AIProvider
from cache.redis_client import RedisClient
from utils.backoff import retry_with_backoff

logger = logging.getLogger(__name__)

# Timeout total del paso 3.2
TIMEOUT_TOTAL = 600


async def analizar_competidores(
    cliente_id: str,
    competidores: list[str],
    cache: RedisClient,
) -> list[dict]:
    """
    Analiza todos los competidores: resuelve URLs, scraping Apify, análisis Gemini.
    Retorna lista de dicts con datos de cada competidor.
    No lanza excepción si un competidor falla — registra datos vacíos.
    """
    logger.info(f"Iniciando análisis de {len(competidores)} competidores para cliente={cliente_id}")

    resultados = []
    for nombre in competidores:
        try:
            resultado = await asyncio.wait_for(
                _analizar_un_competidor(nombre, cache),
                timeout=TIMEOUT_TOTAL // max(len(competidores), 1),
            )
            resultados.append(resultado)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout al analizar competidor '{nombre}'")
            resultados.append(_resultado_vacio(nombre))
        except Exception as e:
            logger.warning(f"Error analizando competidor '{nombre}': {e}")
            resultados.append(_resultado_vacio(nombre))

    # Paso D: Síntesis cruzada (solo si hay competidores con datos)
    competidores_con_datos = [r for r in resultados if r.get("cantidad_anuncios", 0) > 0]
    if competidores_con_datos:
        try:
            sintesis = await _sintesis_cruzada(competidores_con_datos)
            for r in resultados:
                r["oportunidades_detectadas"] = sintesis
        except Exception as e:
            logger.warning(f"Error en síntesis cruzada: {e}")

    logger.info(f"Análisis de competidores completado para cliente={cliente_id}")
    return resultados


async def _analizar_un_competidor(nombre: str, cache: RedisClient) -> dict:
    """Flujo completo para un solo competidor."""
    resultado = {
        "competidor_nombre": nombre,
        "competidor_page_url": None,
        "cantidad_anuncios": 0,
        "anuncios_raw": [],
        "creativos_urls": [],
        "copies": [],
        "formatos_anuncio": [],
        "plataformas": [],
        "creativos_activos": [],
        "tiempo_activo": {},
        "analisis_por_anuncio": [],
        "patrones_messaging": None,
        "sintesis_competitiva": "Sin datos disponibles",
    }

    # Paso A: Resolver nombre → URL de Facebook
    fb_url = await _resolver_url_facebook(nombre, cache)
    if not fb_url:
        logger.warning(f"No se encontró URL de Facebook para '{nombre}'")
        return resultado
    resultado["competidor_page_url"] = fb_url

    # Paso B: Scraping con Apify
    ads_data = await _scraping_apify(fb_url, cache)
    if not ads_data:
        logger.warning(f"Sin datos de Apify para '{nombre}'")
        return resultado

    # Procesar datos del dataset
    anuncios = ads_data if isinstance(ads_data, list) else ads_data.get("items", ads_data)
    if not isinstance(anuncios, list):
        anuncios = []

    resultado["cantidad_anuncios"] = len(anuncios)
    resultado["anuncios_raw"] = anuncios[:50]  # Limitar a 50

    # Extraer creativos, copies, formatos y plataformas
    creativos_urls = []
    copies = []
    formatos = set()
    plataformas = set()
    creativos_activos = []

    for ad in anuncios[:50]:
        snapshot = ad.get("snapshot", {})

        # Copy del anuncio
        body = snapshot.get("body", {})
        if isinstance(body, dict):
            copy_text = body.get("text", "")
        elif isinstance(body, str):
            copy_text = body
        else:
            copy_text = ""
        if copy_text:
            copies.append(copy_text)

        # Formato del anuncio
        display_format = snapshot.get("display_format", "UNKNOWN")
        formatos.add(display_format)

        # Plataformas
        platforms = ad.get("publisher_platform", [])
        if isinstance(platforms, list):
            plataformas.update(platforms)

        # URLs de creativos (imágenes y videos)
        creative_url = None
        images = snapshot.get("images", [])
        videos = snapshot.get("videos", [])

        if videos and isinstance(videos, list):
            video = videos[0]
            if isinstance(video, dict):
                creative_url = video.get("video_hd_url") or video.get("video_sd_url")
        elif images and isinstance(images, list):
            img = images[0]
            if isinstance(img, dict):
                creative_url = img.get("original_image_url") or img.get("resized_image_url")
            elif isinstance(img, str):
                creative_url = img

        if creative_url:
            creativos_urls.append(creative_url)
            creativos_activos.append({
                "url": creative_url,
                "format": display_format,
                "cta": snapshot.get("cta_text"),
                "title": snapshot.get("title"),
            })

    resultado["creativos_urls"] = creativos_urls
    resultado["copies"] = copies
    resultado["formatos_anuncio"] = list(formatos)
    resultado["plataformas"] = list(plataformas)
    resultado["creativos_activos"] = creativos_activos

    # Tiempo activo
    if anuncios:
        resultado["tiempo_activo"] = {
            "primer_anuncio": anuncios[-1].get("start_date_formatted", ""),
            "ultimo_anuncio": anuncios[0].get("start_date_formatted", ""),
        }

    # Paso C: Análisis Gemini Vision por anuncio
    analisis_anuncios = await _analizar_anuncios_gemini(creativos_urls[:50], copies[:50], cache)
    resultado["analisis_por_anuncio"] = analisis_anuncios

    # Paso D: Síntesis por competidor
    if analisis_anuncios:
        try:
            sintesis = await _sintesis_competidor(nombre, analisis_anuncios)
            resultado["patrones_messaging"] = sintesis
            resultado["sintesis_competitiva"] = sintesis
        except Exception as e:
            logger.warning(f"Error en síntesis para '{nombre}': {e}")

    return resultado


# =====================================================================
# PASO A: Resolver nombre → URL de Facebook
# =====================================================================

async def _resolver_url_facebook(nombre: str, cache: RedisClient) -> Optional[str]:
    """Busca la URL de Facebook de un competidor usando SerpAPI."""
    # Verificar cache
    cached = await cache.get_fb_url(nombre)
    if cached:
        logger.info(f"URL de Facebook para '{nombre}' encontrada en cache")
        return cached

    serpapi_key = os.environ.get("SERPAPI_KEY")
    if not serpapi_key:
        logger.warning("SERPAPI_KEY no configurada — no se puede resolver URL de Facebook")
        return None

    query = f'site:facebook.com "{nombre}"'
    params = {
        "q": query,
        "api_key": serpapi_key,
        "hl": "es",
        "gl": "mx",
        "num": 5,
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get("https://serpapi.com/search.json", params=params)
            resp.raise_for_status()
            data = resp.json()

        organic_results = data.get("organic_results", [])
        for result in organic_results:
            url = result.get("link", "")
            if "facebook.com/" in url:
                # Guardar en cache
                await cache.set_fb_url(nombre, url)
                logger.info(f"URL de Facebook resuelta para '{nombre}': {url}")
                return url

        logger.warning(f"No se encontró URL de Facebook para '{nombre}' en SerpAPI")
        return None
    except Exception as e:
        logger.warning(f"Error buscando URL de Facebook para '{nombre}': {e}")
        return None


# =====================================================================
# PASO B: Scraping con Apify
# =====================================================================

async def _scraping_apify(facebook_url: str, cache: RedisClient) -> Optional[list]:
    """Ejecuta el actor de Apify para scraping de Facebook Ad Library."""
    # Verificar cache
    cached = await cache.get_apify_ads(facebook_url)
    if cached:
        logger.info(f"Datos de Apify encontrados en cache para {facebook_url}")
        return cached

    apify_token = os.environ.get("APIFY_API_TOKEN")
    actor_id = os.environ.get("APIFY_ACTOR_ID", "curious_coder/facebook-ads-library-scraper")
    if not apify_token:
        logger.warning("APIFY_API_TOKEN no configurado")
        return None

    # Construir input del actor según schema real confirmado
    actor_input = {
        "urls": [facebook_url],
        "limitPerSource": 50,
        "scrapePageAds.activeStatus": "active",
        "scrapePageAds.countryCode": "MX",
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Iniciar run
            run_url = f"https://api.apify.com/v2/acts/{actor_id}/runs?token={apify_token}"
            resp = await client.post(run_url, json=actor_input)
            resp.raise_for_status()
            run_data = resp.json().get("data", {})
            run_id = run_data.get("id")
            if not run_id:
                logger.warning("Apify no retornó run ID")
                return None

        # Polling del estado
        logger.info(f"Apify run iniciado: {run_id}")
        dataset_id = await _poll_apify_run(run_id, apify_token, timeout=300)
        if not dataset_id:
            return None

        # Obtener dataset
        async with httpx.AsyncClient(timeout=60) as client:
            dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={apify_token}"
            resp = await client.get(dataset_url)
            resp.raise_for_status()
            items = resp.json()

        # Guardar en cache
        await cache.set_apify_ads(facebook_url, items)
        logger.info(f"Apify scraping completado: {len(items)} anuncios obtenidos")
        return items

    except Exception as e:
        logger.warning(f"Error en scraping Apify para {facebook_url}: {e}")
        return None


async def _poll_apify_run(run_id: str, token: str, timeout: int = 300) -> Optional[str]:
    """Polling del estado de un run de Apify. Retorna defaultDatasetId o None."""
    poll_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={token}"
    elapsed = 0
    interval = 10

    async with httpx.AsyncClient(timeout=15) as client:
        while elapsed < timeout:
            await asyncio.sleep(interval)
            elapsed += interval

            try:
                resp = await client.get(poll_url)
                resp.raise_for_status()
                data = resp.json().get("data", {})
                status = data.get("status")

                if status == "SUCCEEDED":
                    return data.get("defaultDatasetId")
                elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
                    logger.warning(f"Apify run {run_id} terminó con estado: {status}")
                    return None
                # RUNNING, READY — seguir esperando
            except Exception as e:
                logger.warning(f"Error polling Apify run {run_id}: {e}")

    logger.warning(f"Timeout esperando Apify run {run_id}")
    return None


# =====================================================================
# PASO C: Análisis Gemini Vision por anuncio
# =====================================================================

AD_ANALYSIS_PROMPT = """Analiza este anuncio de Facebook/Instagram.
Responde SOLO con JSON válido, sin markdown, sin texto extra:
{
  "hook": "elemento o frase que capta la atención inicial",
  "promesa_principal": "beneficio o resultado que promete el anuncio",
  "triggers_psicologicos": ["urgencia", "prueba_social", "escasez", "autoridad"],
  "tono": "profesional|emocional|humoristico|informativo|agresivo|inspiracional",
  "tipo_cta": "texto exacto del CTA si visible, o null",
  "debilidades_detectadas": ["debilidad1"]
}"""


async def _analizar_anuncios_gemini(
    creativos_urls: list[str],
    copies: list[str],
    cache: RedisClient,
) -> list[dict]:
    """Analiza cada anuncio con Gemini. Usa cache por URL de creativo."""
    ai = AIProvider()
    resultados = []

    for i, creative_url in enumerate(creativos_urls[:50]):
        # Verificar cache
        cached = await cache.get_gemini_ad_analysis(creative_url)
        if cached:
            resultados.append(cached)
            continue

        # Construir contexto con el copy del anuncio si disponible
        copy_text = copies[i] if i < len(copies) else "No disponible"
        context = f"Copy del anuncio: {copy_text}\nURL del creativo: {creative_url}"

        try:
            response_text = await ai.generate(
                system_prompt="Eres un analista de publicidad digital experto. Responde ÚNICAMENTE con JSON válido.",
                context=context,
                instruction=AD_ANALYSIS_PROMPT,
                timeout=30,
            )
            # Limpiar y parsear
            cleaned = response_text.strip()
            if cleaned.startswith("```"):
                lines = cleaned.split("\n")
                lines = [l for l in lines if not l.strip().startswith("```")]
                cleaned = "\n".join(lines)

            analisis = json.loads(cleaned)
            await cache.set_gemini_ad_analysis(creative_url, analisis)
            resultados.append(analisis)
        except Exception as e:
            logger.warning(f"Error analizando anuncio {i + 1}: {e}")
            resultados.append({"error": str(e), "creative_url": creative_url})

    return resultados


# =====================================================================
# PASO D: Síntesis por competidor + síntesis cruzada
# =====================================================================

async def _sintesis_competidor(nombre: str, analisis_anuncios: list[dict]) -> str:
    """Genera síntesis narrativa de los anuncios de un competidor."""
    ai = AIProvider()
    # Filtrar análisis válidos
    valid = [a for a in analisis_anuncios if "error" not in a]
    if not valid:
        return "Sin datos suficientes para síntesis"

    context = json.dumps(valid[:20], ensure_ascii=False, indent=1)
    instruction = (
        f"Analiza los siguientes {len(valid)} análisis de anuncios del competidor '{nombre}'. "
        "Identifica: mensajes dominantes, triggers psicológicos más usados, tono predominante "
        "y debilidades principales. Responde en un párrafo narrativo de 3-5 oraciones."
    )

    return await ai.generate(
        system_prompt="Eres un estratega de marketing competitivo.",
        context=context,
        instruction=instruction,
        timeout=45,
    )


async def _sintesis_cruzada(competidores_con_datos: list[dict]) -> str:
    """Genera síntesis cruzada identificando gaps y oportunidades."""
    ai = AIProvider()
    resúmenes = []
    for c in competidores_con_datos:
        resúmenes.append({
            "competidor": c["competidor_nombre"],
            "anuncios": c.get("cantidad_anuncios", 0),
            "sintesis": c.get("sintesis_competitiva", ""),
        })

    context = json.dumps(resúmenes, ensure_ascii=False, indent=1)
    instruction = (
        "Dado el análisis de todos los competidores, identifica: "
        "1) Gaps de posicionamiento, 2) Oportunidades de diferenciación, "
        "3) Recomendación de posicionamiento. Máximo 6 oraciones totales."
    )

    return await ai.generate(
        system_prompt="Eres un estratega de marketing competitivo experto.",
        context=context,
        instruction=instruction,
        timeout=45,
    )


def _resultado_vacio(nombre: str) -> dict:
    """Retorna un registro vacío para un competidor que falló."""
    return {
        "competidor_nombre": nombre,
        "competidor_page_url": None,
        "cantidad_anuncios": 0,
        "anuncios_raw": [],
        "creativos_urls": [],
        "copies": [],
        "formatos_anuncio": [],
        "plataformas": [],
        "creativos_activos": [],
        "tiempo_activo": {},
        "analisis_por_anuncio": [],
        "patrones_messaging": None,
        "oportunidades_detectadas": None,
        "sintesis_competitiva": "Sin datos disponibles",
    }
