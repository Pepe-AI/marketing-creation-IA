"""
services/keywords.py — Etapa 3.3 + 3.4: Keyword Research (SerpAPI + Keyword Planner).
"""

import os
import json
import logging
from typing import Optional

import httpx

from ai.provider import AIProvider
from db.client import DatabaseClient
from cache.redis_client import RedisClient
from utils.backoff import retry_with_backoff

logger = logging.getLogger(__name__)


async def researchar_keywords(
    cliente_id: str,
    keywords_base: list[str],
    keywords_transcript: list[str],
    datos_form: dict,
    db: DatabaseClient,
    cache: RedisClient,
) -> None:
    """
    Construye pool de keywords, ejecuta SerpAPI y Google Keyword Planner.
    Ningún error aquí detiene el pipeline — todo es non-critical.
    """
    logger.info(f"Iniciando keyword research para cliente={cliente_id}")

    # Construir pool de keywords unificado
    pool = await _construir_pool_keywords(keywords_base, keywords_transcript, datos_form)
    logger.info(f"Pool de keywords construido: {len(pool)} keywords únicas")

    # 3.3 — SerpAPI por keyword
    for keyword in pool:
        try:
            await _buscar_serp(cliente_id, keyword, db, cache)
        except Exception as e:
            logger.warning(f"Error SerpAPI para keyword '{keyword}': {e}")

    # 3.4 — Google Keyword Planner (graceful degradation total)
    try:
        await _keyword_planner(cliente_id, pool, db, cache)
    except Exception as e:
        logger.warning(f"Keyword Planner falló completamente (graceful degradation): {e}")

    logger.info(f"Keyword research completado para cliente={cliente_id}")


async def _construir_pool_keywords(
    keywords_base: list[str],
    keywords_transcript: list[str],
    datos_form: dict,
) -> list[str]:
    """
    Combina y deduplica keywords de múltiples fuentes.
    Límite estricto: 20 keywords máximo.
    """
    pool = set()

    # 1. Nombres de productos_top3
    productos = datos_form.get("productos_top3", [])
    if isinstance(productos, str):
        try:
            productos = json.loads(productos)
        except json.JSONDecodeError:
            productos = []
    for p in productos:
        if isinstance(p, str):
            pool.add(p.strip().lower())
        elif isinstance(p, dict):
            nombre = p.get("nombre", p.get("name", ""))
            if nombre:
                pool.add(nombre.strip().lower())

    # 2. Sustantivos clave del propósito de negocio
    proposito = datos_form.get("proposito_negocio", "")
    if proposito:
        try:
            ai = AIProvider()
            response = await ai.generate(
                system_prompt="Eres un experto en SEO. Responde SOLO con un array JSON de strings.",
                context="",
                instruction=(
                    f"Del siguiente texto, extrae de 3 a 5 sustantivos clave que representen "
                    f"el negocio o industria. Responde SOLO con un array JSON: "
                    f'[\"palabra1\", \"palabra2\", ...]\n\nTexto: {proposito}'
                ),
                timeout=30,
            )
            cleaned = response.strip()
            if cleaned.startswith("```"):
                lines = cleaned.split("\n")
                lines = [l for l in lines if not l.strip().startswith("```")]
                cleaned = "\n".join(lines)
            sustantivos = json.loads(cleaned)
            for s in sustantivos:
                if isinstance(s, str):
                    pool.add(s.strip().lower())
        except Exception as e:
            logger.warning(f"Error extrayendo sustantivos del propósito: {e}")

    # 3. Keywords del transcript
    for kw in keywords_transcript:
        if isinstance(kw, str):
            pool.add(kw.strip().lower())

    # 4. Keywords base (de datos_form u otras fuentes)
    for kw in keywords_base:
        if isinstance(kw, str):
            pool.add(kw.strip().lower())

    # Filtrar vacíos y limitar a 20
    pool = [kw for kw in pool if kw]
    return pool[:20]


async def _buscar_serp(
    cliente_id: str,
    keyword: str,
    db: DatabaseClient,
    cache: RedisClient,
) -> None:
    """Busca en SerpAPI y guarda resultados."""
    # Verificar cache
    cached = await cache.get_serp(keyword)
    if cached:
        logger.info(f"SerpAPI cache hit para '{keyword}'")
        await db.guardar_keyword_serp(cliente_id, keyword, cached)
        return

    serpapi_key = os.environ.get("SERPAPI_KEY")
    if not serpapi_key:
        logger.warning("SERPAPI_KEY no configurada")
        return

    params = {
        "q": keyword,
        "api_key": serpapi_key,
        "hl": "es",
        "gl": "mx",
        "num": 10,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get("https://serpapi.com/search.json", params=params)
        resp.raise_for_status()
        data = resp.json()

    # Extraer datos relevantes
    datos = {
        "resultados_organicos": data.get("organic_results", [])[:10],
        "anuncios_encontrados": data.get("ads", [])[:5],
        "people_also_ask": data.get("related_questions", [])[:5],
        "busquedas_relacionadas": [
            r.get("query", "") for r in data.get("related_searches", [])[:10]
        ],
        "sugerencias_autocomplete": [
            s.get("value", "") for s in data.get("suggestions", [])[:10]
        ],
        "tipo_contenido_posiciona": _detectar_tipo_contenido(data.get("organic_results", [])),
    }

    # Guardar en cache y DB
    await cache.set_serp(keyword, datos)
    await db.guardar_keyword_serp(cliente_id, keyword, datos)
    logger.info(f"SerpAPI completado para '{keyword}'")


def _detectar_tipo_contenido(organic_results: list) -> str:
    """Detecta el tipo de contenido que posiciona mejor basado en los resultados."""
    if not organic_results:
        return "desconocido"

    tipos = {"blog": 0, "ecommerce": 0, "video": 0, "directorio": 0, "otro": 0}
    for r in organic_results[:5]:
        link = r.get("link", "").lower()
        snippet = r.get("snippet", "").lower()
        if "youtube.com" in link or "video" in snippet:
            tipos["video"] += 1
        elif any(kw in link for kw in ["/blog", "/articulo", "/post"]):
            tipos["blog"] += 1
        elif any(kw in link for kw in ["/product", "/shop", "/tienda", "mercadolibre"]):
            tipos["ecommerce"] += 1
        else:
            tipos["otro"] += 1

    return max(tipos, key=tipos.get)


async def _keyword_planner(
    cliente_id: str,
    keywords: list[str],
    db: DatabaseClient,
    cache: RedisClient,
) -> None:
    """
    Google Keyword Planner — GRACEFUL DEGRADATION TOTAL.
    Si falla por cualquier razón, los campos quedan NULL.
    """
    try:
        credentials_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        if not credentials_json:
            logger.warning("GOOGLE_CREDENTIALS_JSON no configurada — Keyword Planner omitido")
            return

        from google.auth import credentials as auth_creds
        from google.oauth2 import service_account

        creds_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/adwords"],
        )

        # Google Ads API requiere configuración adicional (customer ID, developer token)
        # que normalmente no está disponible — esto es graceful degradation intencional
        logger.warning(
            "Google Keyword Planner requiere Google Ads API configurado. "
            "Campos volumen/competencia/CPC quedarán NULL."
        )

    except Exception as e:
        logger.warning(f"Keyword Planner no disponible: {e}")
