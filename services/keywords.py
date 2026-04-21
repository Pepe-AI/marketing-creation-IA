"""
services/keywords.py — Etapa 3.3 + 3.4: Keyword Research
(SerpAPI + pytrends + Gemini batch + Keyword Planner).

Flujo:
  3.3a — SerpAPI por keyword (paralelo con 3.3b)
  3.3b — pytrends por keyword (paralelo con 3.3a)
  3.3c — Gemini batch analysis (secuencial, después de 3.2 + 3.3a/b)
  3.4  — Google Keyword Planner (condicional, solo si credenciales configuradas)
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


# ═══════════════════════════════════════════════════════════════════
# API PÚBLICA
# ═══════════════════════════════════════════════════════════════════


async def researchar_keywords(
    cliente_id: str,
    keywords_base: list[str],
    keywords_transcript: list[str],
    datos_form: dict,
    db: DatabaseClient,
    cache: RedisClient,
) -> dict:
    """
    3.3a + 3.3b: Construye pool, ejecuta SerpAPI + pytrends en paralelo por keyword.
    Retorna datos acumulados para que main.py los pase a gemini_batch_keywords.
    """
    logger.info(f"Iniciando keyword research para cliente={cliente_id}")

    pool = await _construir_pool_keywords(keywords_base, keywords_transcript, datos_form)
    logger.info(f"Pool de keywords construido: {len(pool)} keywords únicas")

    # Geolocalización basada en mercado_objetivo del cliente
    mercado_objetivo = (datos_form.get("mercado_objetivo") or "mx").strip()
    gl_serp = mercado_objetivo.lower()
    geo_pytrends = mercado_objetivo.upper()

    serp_results = {}
    pytrends_results = {}

    for i, keyword in enumerate(pool):
        # TEMPORAL — PLAN FREE / pytrends no oficial:
        # Borrar el delay de 1.5s entre keywords cuando se confirme
        # en producción que Google no bloquea las consultas de pytrends.
        if i > 0:
            await asyncio.sleep(1.5)

        # Paralelo interno: SerpAPI (3.3a) + pytrends (3.3b) por keyword
        results = await asyncio.gather(
            _buscar_serp(cliente_id, keyword, db, cache, gl_serp),
            _buscar_pytrends(keyword, cache, geo_pytrends),
            return_exceptions=True,
        )

        serp_res, pytrends_res = results

        # SerpAPI
        if isinstance(serp_res, Exception):
            logger.warning(f"Error SerpAPI para keyword '{keyword}': {serp_res}")
        elif serp_res:
            serp_results[keyword] = serp_res

        # pytrends
        if isinstance(pytrends_res, Exception):
            logger.warning(f"Error pytrends para keyword '{keyword}': {pytrends_res}")
        elif pytrends_res:
            pytrends_results[keyword] = pytrends_res
            try:
                await db.actualizar_keyword_pytrends(
                    cliente_id, keyword,
                    pytrends_res.get("interes_relativo"),
                    pytrends_res.get("tendencia_12m"),
                )
            except Exception as e:
                logger.warning(f"Error guardando pytrends en DB para '{keyword}': {e}")

    pytrends_available = bool(pytrends_results)
    if not pytrends_available:
        logger.warning("pytrends falló para todas las keywords — continuando sin datos de tendencia")

    logger.info(
        f"Keyword research base completado: "
        f"{len(serp_results)} SERP + {len(pytrends_results)} pytrends"
    )

    return {
        "pool": pool,
        "serp_results": serp_results,
        "pytrends_results": pytrends_results,
        "pytrends_available": pytrends_available,
    }


async def gemini_batch_keywords(
    cliente_id: str,
    pool: list[str],
    serp_results: dict,
    pytrends_results: dict,
    pytrends_available: bool,
    db: DatabaseClient,
    cache: RedisClient,
) -> None:
    """
    3.3c — Un solo prompt a Gemini con datos SERP + pytrends de TODAS las keywords.
    Se ejecuta DESPUÉS de que 3.2 (competidores) y 3.3a/b (SerpAPI + pytrends)
    hayan terminado, para evitar errores 429 en el plan gratuito de Gemini.
    """
    try:
        # Verificar cache
        cached = await cache.get_gemini_kw_analysis(cliente_id)
        if cached:
            logger.info(f"Gemini keyword analysis cache hit para cliente={cliente_id}")
            await _guardar_gemini_analysis_en_db(cliente_id, cached, pytrends_available, pool, db)
            return

        # Construir datos por keyword para el prompt
        keywords_data = []
        for keyword in pool:
            kw_info = {"keyword": keyword}

            serp = serp_results.get(keyword, {})
            if serp:
                organicos = serp.get("resultados_organicos", [])[:5]
                kw_info["top_resultados"] = [
                    {"titulo": r.get("title", ""), "url": r.get("link", "")}
                    for r in organicos
                ]
                kw_info["cantidad_anuncios"] = len(serp.get("anuncios_encontrados", []))
                kw_info["preguntas_paa"] = [
                    q.get("question", q) if isinstance(q, dict) else str(q)
                    for q in serp.get("people_also_ask", [])
                ]
                kw_info["busquedas_relacionadas"] = serp.get("busquedas_relacionadas", [])

            pt = pytrends_results.get(keyword)
            if pt:
                kw_info["interes_relativo"] = pt.get("interes_relativo")
                # Determinar dirección de tendencia a partir de tendencia_12m
                tendencia_12m = pt.get("tendencia_12m", [])
                if tendencia_12m and len(tendencia_12m) >= 3:
                    first_3 = sum(v.get("valor", 0) for v in tendencia_12m[:3]) / 3
                    last_3 = sum(v.get("valor", 0) for v in tendencia_12m[-3:]) / 3
                    if last_3 > first_3 * 1.1:
                        kw_info["tendencia"] = "subiendo"
                    elif last_3 < first_3 * 0.9:
                        kw_info["tendencia"] = "bajando"
                    else:
                        kw_info["tendencia"] = "estable"

            keywords_data.append(kw_info)

        if not keywords_data:
            logger.warning("No hay datos de keywords para análisis Gemini batch")
            return

        ai = AIProvider()
        response = await retry_with_backoff(
            lambda: ai.generate(
                system_prompt=(
                    "Eres un experto en SEO y marketing digital.\n"
                    "Responde ÚNICAMENTE con JSON válido, sin markdown, sin texto extra."
                ),
                context="",
                instruction=(
                    "Analiza el siguiente conjunto de keywords para un negocio y para "
                    "cada una proporciona:\n\n"
                    "1. intencion_busqueda: clasificar como exactamente uno de estos valores: "
                    "'informational', 'transactional', 'navigational' o 'commercial'\n\n"
                    "2. analisis_serp_ia: texto narrativo de 2-3 oraciones describiendo:\n"
                    "   - nivel de demanda estimada (alta/media/baja)\n"
                    "   - competencia observada en el SERP\n"
                    "   - oportunidades de posicionamiento detectadas\n\n"
                    f"Datos de keywords:\n{json.dumps(keywords_data, ensure_ascii=False, indent=2)}\n\n"
                    "Responde SOLO con este JSON:\n"
                    "{\n"
                    '  "resultados": [\n'
                    "    {\n"
                    '      "keyword": "término exacto",\n'
                    '      "intencion_busqueda": "...",\n'
                    '      "analisis_serp_ia": "..."\n'
                    "    }\n"
                    "  ]\n"
                    "}"
                ),
                timeout=90,
                max_output_tokens=16384,  # 48 keywords × ~200 tokens c/u
            ),
            max_retries=2,
            base_delay=3.0,
        )

        # Parsear respuesta (limpiar markdown si Gemini lo envuelve)
        cleaned = response.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines)

        resultado = json.loads(cleaned)

        # Guardar en cache y DB
        await cache.set_gemini_kw_analysis(cliente_id, resultado)
        await _guardar_gemini_analysis_en_db(cliente_id, resultado, pytrends_available, pool, db)

        logger.info(f"Gemini batch keyword analysis completado para cliente={cliente_id}")

    except Exception as e:
        logger.warning(f"Gemini batch keyword analysis falló (graceful degradation): {e}")


async def keyword_volumes_kwp(
    cliente_id: str,
    pool: list[str],
    db: DatabaseClient,
    cache: RedisClient,
) -> None:
    """
    3.4 — Google Keyword Planner (completamente opcional).
    Skip silencioso si las credenciales no están configuradas.
    """
    required_vars = [
        "GOOGLE_ADS_DEVELOPER_TOKEN",
        "GOOGLE_ADS_CLIENT_ID",
        "GOOGLE_ADS_CLIENT_SECRET",
        "GOOGLE_ADS_REFRESH_TOKEN",
        "GOOGLE_ADS_CUSTOMER_ID",
    ]

    missing = [v for v in required_vars if not os.environ.get(v, "").strip()]
    if missing:
        logger.info("Keyword Planner no configurado — skip silencioso")
        return

    try:
        # TODO: implementar llamada real a Google Ads API / Keyword Planner
        logger.info("Keyword Planner credenciales presentes — implementación pendiente")
        # Cuando KWP funcione, actualizar fuente_volumen = 'keyword_planner' para cada keyword:
        # for keyword in pool:
        #     datos = await _consultar_kwp(keyword)
        #     if datos:
        #         await db.actualizar_keyword_volumenes(cliente_id, keyword, datos)
        #         await db.actualizar_keyword_gemini_analysis(
        #             cliente_id, keyword, None, None, 'keyword_planner')
    except Exception as e:
        logger.warning(f"Keyword Planner falló en runtime (graceful degradation): {e}")


# ═══════════════════════════════════════════════════════════════════
# HELPERS PRIVADOS
# ═══════════════════════════════════════════════════════════════════


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
    gl: str = "mx",
) -> Optional[dict]:
    """Busca en SerpAPI con geolocalización dinámica y guarda resultados. Retorna los datos."""
    # Verificar cache
    cached = await cache.get_serp(keyword)
    if cached:
        logger.info(f"SerpAPI cache hit para '{keyword}'")
        await db.guardar_keyword_serp(cliente_id, keyword, cached)
        return cached

    serpapi_key = os.environ.get("SERPAPI_KEY")
    if not serpapi_key:
        logger.warning("SERPAPI_KEY no configurada")
        return None

    params = {
        "q": keyword,
        "api_key": serpapi_key,
        "hl": "es",
        "gl": gl,  # Geolocalización dinámica basada en mercado_objetivo
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
    return datos


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


async def _buscar_pytrends(
    keyword: str,
    cache: RedisClient,
    geo: str,
) -> Optional[dict]:
    """
    Consulta Google Trends via pytrends para una keyword.
    Retorna interes_relativo (0-100) + tendencia_12m (array mensual).
    """
    cached = await cache.get_pytrends(keyword)
    if cached:
        logger.info(f"pytrends cache hit para '{keyword}'")
        return cached

    # pytrends es síncrono — ejecutar en thread pool
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _pytrends_sync, keyword, geo)

    if result:
        await cache.set_pytrends(keyword, result)

    return result


def _pytrends_sync(keyword: str, geo: str) -> Optional[dict]:
    """Ejecuta consulta pytrends de forma síncrona (se llama desde executor)."""
    from pytrends.request import TrendReq

    pytrends_client = TrendReq(hl='es', tz=360)
    pytrends_client.build_payload([keyword], timeframe='today 12-m', geo=geo)

    interest_df = pytrends_client.interest_over_time()
    if interest_df.empty or keyword not in interest_df.columns:
        return None

    values = interest_df[keyword].tolist()
    dates = interest_df.index.tolist()

    interes_relativo = max(values) if values else 0

    # Agregar datos semanales por mes (pytrends devuelve datos semanales)
    monthly = {}
    for date, value in zip(dates, values):
        mes = date.strftime("%Y-%m")
        if mes not in monthly:
            monthly[mes] = []
        monthly[mes].append(int(value))

    tendencia_12m = [
        {"mes": mes, "valor": int(sum(vals) / len(vals))}
        for mes, vals in sorted(monthly.items())
    ][-12:]  # últimos 12 meses

    return {
        "interes_relativo": interes_relativo,
        "tendencia_12m": tendencia_12m,
    }


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


async def _guardar_gemini_analysis_en_db(
    cliente_id: str,
    resultado: dict,
    pytrends_available: bool,
    pool: list[str],
    db: DatabaseClient,
) -> None:
    """Guarda análisis Gemini batch en la tabla keyword_research."""
    resultados = resultado.get("resultados", [])
    analisis_map = {r["keyword"].lower(): r for r in resultados if "keyword" in r}

    for keyword in pool:
        analisis = analisis_map.get(keyword.lower(), {})
        intencion = analisis.get("intencion_busqueda")
        analisis_text = analisis.get("analisis_serp_ia")
        fuente_volumen = "pytrends_gemini" if pytrends_available else "serp_only"

        try:
            await db.actualizar_keyword_gemini_analysis(
                cliente_id, keyword, intencion, analisis_text, fuente_volumen
            )
        except Exception as e:
            logger.warning(f"Error guardando Gemini analysis para '{keyword}': {e}")
