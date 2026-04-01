"""
services/keywords.py — Etapa 3.3 + 3.4: Keyword Research (SerpAPI + Keyword Planner).
"""

import os
import re
import json
import asyncio
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

    # 3.3 — SerpAPI por keyword (cada _buscar_serp llama a Gemini para clasificar tipo_contenido)
    for i, keyword in enumerate(pool):
        # TEMPORAL — PLAN FREE GEMINI: borrar toda la lógica del delay
        # una vez se actualice al plan de pago de Gemini API.
        if i > 0:
            await asyncio.sleep(1.5)
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

    # Filtrar vacíos, limpiar y limitar a 20
    pool = [kw for kw in pool if kw]
    pool = _limpiar_keywords(pool)
    return pool[:20]


_LETRAS_ES = re.compile(r'[a-záéíóúñü]', re.IGNORECASE)


def _limpiar_keywords(pool: list[str]) -> list[str]:
    """
    Filtra keywords inválidas usando regex y lógica determinista.
    Sin llamadas a APIs externas.
    """
    n_original = len(pool)
    resultado = []
    vistos = set()

    for kw in pool:
        kw = kw.strip()

        # REGLA 1 — Longitud mínima: 4 caracteres
        if len(kw) < 4:
            continue

        # REGLA 2 — Mínimo 2 letras del alfabeto español
        letras = _LETRAS_ES.findall(kw)
        if len(letras) < 2:
            continue

        # REGLA 3 — Letras deben ser al menos 60% del total
        if len(letras) / len(kw) < 0.6:
            continue

        # REGLA 4 — Deduplicación case-insensitive
        clave = kw.lower()
        if clave in vistos:
            continue
        vistos.add(clave)

        resultado.append(kw)

    descartadas = n_original - len(resultado)
    if descartadas > 0:
        logger.info(
            f"Pool de keywords: {n_original} candidatas → "
            f"{len(resultado)} válidas tras filtrado"
        )

    return resultado


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
    organicos = data.get("organic_results", [])[:10]
    tipo_contenido = await _detectar_tipo_contenido(keyword, organicos)

    datos = {
        "resultados_organicos": organicos,
        "anuncios_encontrados": data.get("ads", [])[:5],
        "people_also_ask": data.get("related_questions", [])[:5],
        "busquedas_relacionadas": [
            r.get("query", "") for r in data.get("related_searches", [])[:10]
        ],
        "sugerencias_autocomplete": [
            s.get("value", "") for s in data.get("suggestions", [])[:10]
        ],
        "tipo_contenido_posiciona": tipo_contenido,
    }

    # Guardar en cache y DB
    await cache.set_serp(keyword, datos)
    await db.guardar_keyword_serp(cliente_id, keyword, datos)
    logger.info(f"SerpAPI completado para '{keyword}'")


async def _buscar_serp_solo(keyword: str, cache: RedisClient) -> Optional[dict]:
    """Busca en SerpAPI y guarda en cache. No toca DB — para scripts de test."""
    serpapi_key = os.environ.get("SERPAPI_KEY")
    if not serpapi_key:
        logger.warning("SERPAPI_KEY no configurada")
        return None

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

    organicos = data.get("organic_results", [])[:10]
    tipo_contenido = await _detectar_tipo_contenido(keyword, organicos)

    datos = {
        "resultados_organicos": organicos,
        "anuncios_encontrados": data.get("ads", [])[:5],
        "people_also_ask": data.get("related_questions", [])[:5],
        "busquedas_relacionadas": [
            r.get("query", "") for r in data.get("related_searches", [])[:10]
        ],
        "sugerencias_autocomplete": [
            s.get("value", "") for s in data.get("suggestions", [])[:10]
        ],
        "tipo_contenido_posiciona": tipo_contenido,
    }

    await cache.set_serp(keyword, datos)
    return datos


async def _detectar_tipo_contenido(keyword: str, organic_results: list) -> str:
    """Clasifica el tipo de contenido dominante usando Gemini."""
    if not organic_results:
        return "desconocido"

    # Construir lista de títulos + URLs para el prompt
    resultados_texto = "\n".join(
        f"- {r.get('title', 'Sin título')} | {r.get('link', '')}"
        for r in organic_results[:5]
    )

    try:
        ai = AIProvider()
        respuesta = await ai.generate(
            system_prompt="Eres un analista SEO experto. Responde con una sola palabra o frase corta.",
            context="",
            instruction=(
                f'Dados estos resultados de búsqueda de Google para la keyword "{keyword}":\n'
                f"{resultados_texto}\n\n"
                "Clasifica en UNA sola palabra o frase corta qué tipo de contenido "
                "domina los resultados. Ejemplos posibles: \"agencia_servicios\", "
                "\"ecommerce\", \"blog_informativo\", \"directorio\", \"red_social\", "
                "\"noticias\", \"gobierno\", \"educativo\", \"marketplace\".\n\n"
                "Responde SOLO con la clasificación, sin explicación ni texto extra."
            ),
            timeout=10,
        )
        return respuesta.strip().strip('"').lower()
    except Exception as e:
        logger.warning(f"Error clasificando tipo de contenido para '{keyword}': {e}")
        return "no_clasificado"


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
