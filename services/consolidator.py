"""
services/consolidator.py — Etapa 4: Consolidación de datos y generación del prompt.

Consulta las 4 tablas fuente (datos_form, analisis_transcript, analisis_competidores,
keyword_research) para un cliente_id, estructura los datos en 4 bloques temáticos,
construye el prompt completo para la IA, y lo persiste en metodologias_generadas.

v2.1: Incluye campos de pytrends, Gemini batch analysis y mercado_objetivo.
"""

import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Campos esperados de datos_form (para defaults cuando no hay datos)
CAMPOS_FORM = [
    "proposito_negocio", "diferenciadores", "descripcion_deseada",
    "productos_top3", "producto_exclusivo", "precios_promedio",
    "ofertas_especiales", "publico_objetivo", "problemas_que_resuelve",
    "competidores", "errores_competencia", "productos_a_promocionar",
    "mercado_objetivo",
]


# =====================================================================
# Paso 1 — Queries a las 4 tablas fuente
# =====================================================================

async def _obtener_datos_fuente(cliente_id: str, db) -> dict:
    """
    Ejecuta las 4 queries usando los métodos existentes de db/client.py.
    Nunca falla por datos faltantes — retorna defaults si una tabla está vacía.
    """
    # Query 1 — datos_form (1 registro por cliente)
    datos_form = await db.obtener_datos_form(cliente_id)
    if not datos_form:
        logger.warning(f"cliente={cliente_id} — Sin datos_form, usando defaults")
        datos_form = {campo: "No disponible" for campo in CAMPOS_FORM}
        datos_form["competidores"] = []
        datos_form["productos_top3"] = []
        datos_form["productos_a_promocionar"] = []
        datos_form["mercado_objetivo"] = None
    else:
        logger.info(f"cliente={cliente_id} — datos_form: {len([v for v in datos_form.values() if v])} campos con datos")

    # Query 2 — analisis_transcript (1 registro por cliente, SIN transcript_raw)
    transcript = await db.obtener_analisis_transcript(cliente_id)
    if not transcript:
        logger.warning(f"cliente={cliente_id} — Sin analisis_transcript, usando defaults")
        transcript = {
            "resumen_ejecutivo": "No se realizó análisis del transcript",
            "objetivos_cliente": [],
            "keywords_mencionadas": [],
            "metodologias_discutidas": [],
            "acuerdos_decisiones": [],
            "tono_personalidad": "No disponible",
        }
    else:
        # Excluir transcript_raw — es el texto original sin procesar y puede ser enorme
        transcript.pop("transcript_raw", None)
        logger.info(f"cliente={cliente_id} — analisis_transcript: cargado")

    # Query 3 — analisis_competidores (N registros, 1 por competidor)
    competidores_rows = await db.obtener_competidores(cliente_id)
    if not competidores_rows:
        logger.warning(f"cliente={cliente_id} — Sin analisis_competidores")
    else:
        logger.info(f"cliente={cliente_id} — competidores: {len(competidores_rows)} competidores")

    # Query 4 — keyword_research (N registros, 1 por keyword)
    keywords_rows = await db.obtener_keywords(cliente_id)
    if not keywords_rows:
        logger.warning(f"cliente={cliente_id} — Sin keyword_research")
    else:
        logger.info(f"cliente={cliente_id} — keywords: {len(keywords_rows)} keywords")

    return {
        "datos_form": datos_form,
        "transcript": transcript,
        "competidores_rows": competidores_rows,
        "keywords_rows": keywords_rows,
    }


# =====================================================================
# Paso 2 — Estructurar en 4 bloques temáticos
# =====================================================================

def _extraer_sintesis_cruzada(competidores_rows: list) -> str:
    """
    Las oportunidades_detectadas suelen ser idénticas entre competidores
    (porque es el resultado de la síntesis cruzada de Gemini en Etapa 3).
    Extraer una sola copia. Si son diferentes, concatenar todas.
    """
    oportunidades = set()
    for comp in competidores_rows:
        if comp.get("oportunidades_detectadas"):
            oportunidades.add(comp["oportunidades_detectadas"])

    if not oportunidades:
        return "No se detectaron oportunidades cruzadas."

    return "\n\n".join(oportunidades)


def _estructurar_bloques(fuentes: dict) -> dict:
    """
    Toma los datos crudos de las 4 queries y los reorganiza en 4 bloques
    temáticos con nombres descriptivos y campos limpios.
    v2.1: Incluye mercado_objetivo, campos pytrends y Gemini batch.
    """
    datos_form = fuentes["datos_form"]
    transcript = fuentes["transcript"]
    competidores_rows = fuentes["competidores_rows"]
    keywords_rows = fuentes["keywords_rows"]

    # --- Bloque 1: Información del Negocio (de datos_form) ---
    # competidores_declarados: extraer nombres del array de objetos {"nombre": "X"}
    competidores_raw = datos_form.get("competidores", [])
    if isinstance(competidores_raw, list):
        competidores_nombres = [
            c["nombre"] for c in competidores_raw
            if isinstance(c, dict) and "nombre" in c
        ]
    else:
        competidores_nombres = []

    informacion_del_negocio = {
        "proposito": datos_form.get("proposito_negocio", "No disponible"),
        "diferenciadores": datos_form.get("diferenciadores", "No disponible"),
        "descripcion_deseada": datos_form.get("descripcion_deseada", "No disponible"),
        "productos_principales": datos_form.get("productos_top3", []),
        "producto_exclusivo": datos_form.get("producto_exclusivo", "No disponible"),
        "precios": datos_form.get("precios_promedio", "No disponible"),
        "ofertas": datos_form.get("ofertas_especiales", "No disponible"),
        "publico_objetivo": datos_form.get("publico_objetivo", "No disponible"),
        "problemas_que_resuelve": datos_form.get("problemas_que_resuelve", "No disponible"),
        "competidores_declarados": competidores_nombres,
        "errores_competencia": datos_form.get("errores_competencia", "No disponible"),
        "productos_a_promocionar": datos_form.get("productos_a_promocionar", []),
        "mercado_objetivo": datos_form.get("mercado_objetivo"),  # v2.1 — puede ser None
    }

    # --- Bloque 2: Insights de la Reunión (de analisis_transcript) ---
    insights_de_la_reunion = {
        "resumen_ejecutivo": transcript.get("resumen_ejecutivo", "No disponible"),
        "objetivos_cliente": transcript.get("objetivos_cliente", []),
        "keywords_mencionadas": transcript.get("keywords_mencionadas", []),
        "metodologias_discutidas": transcript.get("metodologias_discutidas", []),
        "acuerdos_y_decisiones": transcript.get("acuerdos_decisiones", []),
        "tono_y_personalidad": transcript.get("tono_personalidad", "No disponible"),
    }

    # --- Bloque 3: Inteligencia Competitiva (de analisis_competidores) ---
    # Solo campos interpretados: síntesis, patrones, oportunidades
    # NO incluir: anuncios_raw, creativos_urls, analisis_por_anuncio, competidor_page_url
    competidores_analizados = [
        {
            "nombre": comp.get("competidor_nombre", "Desconocido"),
            "anuncios_analizados": comp.get("cantidad_anuncios", 0),
            "sintesis": comp.get("sintesis_competitiva", ""),
            "patrones": comp.get("patrones_messaging", ""),
            "oportunidades": comp.get("oportunidades_detectadas", ""),
        }
        for comp in competidores_rows
    ]

    inteligencia_competitiva = {
        "competidores_analizados": competidores_analizados,
        "sintesis_cruzada": _extraer_sintesis_cruzada(competidores_rows),
    }

    # --- Bloque 4: Inteligencia de Keywords (de keyword_research) ---
    # v2.1: Renombrado de "investigacion_de_keywords" a "inteligencia_de_keywords"
    # people_also_ask: extraer SOLO campo "question" de cada objeto
    # resultados_organicos: solo top 3, solo título y snippet
    keywords_estructuradas = [
        {
            "keyword": kw.get("keyword", ""),
            # Datos cuantitativos (KWE / KWP) — pueden ser None
            "volumen": kw.get("volumen_busqueda"),
            "competencia": kw.get("nivel_competencia"),
            "cpc": float(kw["cpc_estimado"]) if kw.get("cpc_estimado") else None,
            # Datos pytrends (v2.1) — pueden ser None
            "interes_relativo": kw.get("interes_relativo"),
            "tendencia_12m": kw.get("tendencia_12m") or [],
            # Análisis Gemini (v2.1) — pueden ser None
            "intencion_busqueda": kw.get("intencion_busqueda"),
            "analisis_serp_ia": kw.get("analisis_serp_ia"),
            # Datos SerpAPI (siempre disponibles)
            "preguntas_frecuentes": [
                item["question"]
                for item in (kw.get("people_also_ask") or [])
                if isinstance(item, dict) and "question" in item
            ],
            "busquedas_relacionadas": kw.get("busquedas_relacionadas") or [],
            "cantidad_anuncios_pagados": len(kw.get("anuncios_encontrados") or []),
            "top_resultados": [
                {"titulo": r.get("title", ""), "snippet": r.get("snippet", "")}
                for r in (kw.get("resultados_organicos") or [])[:3]
            ],
            "sugerencias": kw.get("sugerencias_autocomplete") or [],
            # Trazabilidad
            "fuente_volumen": kw.get("fuente_volumen"),
        }
        for kw in keywords_rows
    ]

    inteligencia_de_keywords = {
        "keywords": keywords_estructuradas,
    }

    return {
        "informacion_del_negocio": informacion_del_negocio,
        "insights_de_la_reunion": insights_de_la_reunion,
        "inteligencia_competitiva": inteligencia_competitiva,
        "inteligencia_de_keywords": inteligencia_de_keywords,
    }


# =====================================================================
# Paso 3 — Construir el prompt completo
# =====================================================================

# 3A — System Prompt
SYSTEM_PROMPT = """Eres un estratega de marketing digital senior con más de 15 años de experiencia creando metodologías de marketing para empresas en Latinoamérica.

Tu tarea es generar una METODOLOGÍA DE MARKETING completa y profesional para un cliente específico, basándote en los datos proporcionados.

REGLAS:
1. Escribe en español profesional, sin jerga innecesaria
2. Cada sección debe ser actionable — no generalidades vagas
3. Usa los datos reales del cliente en cada sección — NO inventes información
4. Si un dato no está disponible, indícalo y haz tu mejor recomendación basada en lo que sí tienes
5. Las recomendaciones de canales y contenido deben basarse en la inteligencia competitiva real
6. Los KPIs deben ser medibles y realistas para el tamaño del negocio
7. El timeline debe alinearse con los acuerdos y decisiones tomados en la reunión
8. El tono del documento debe reflejar la personalidad de marca detectada

FORMATO DE RESPUESTA:
Genera las 9 secciones numeradas. Cada sección con su título como encabezado.
Usa párrafos narrativos, no listas de bullets excesivas.
Incluye tablas donde sea apropiado (keywords, KPIs, timeline).
Extensión total: 3000-5000 palabras.
"""

# 3C — Instrucciones por sección (template de las 9 secciones)
INSTRUCCIONES_SECCIONES = """
Genera las siguientes 9 secciones de la metodología de marketing:

SECCIÓN 1 — ANÁLISIS DE SITUACIÓN
Usa: INFORMACIÓN DEL NEGOCIO + INSIGHTS DE LA REUNIÓN
Describe la situación actual del negocio: qué hace, su mercado, sus fortalezas y debilidades internas, y el contexto externo. Incluye los desafíos mencionados en la reunión. Si se especifica mercado objetivo (país), contextualiza el análisis para ese mercado.

SECCIÓN 2 — AUDIENCIA OBJETIVO
Usa: público_objetivo + problemas_que_resuelve de INFORMACIÓN DEL NEGOCIO
Define el perfil detallado de la audiencia: demografía, psicografía, comportamientos. Describe los problemas que enfrenta esta audiencia y cómo el negocio los resuelve. Si hay segmentos múltiples (como público cautivo vs nuevo), diferéncialos.

SECCIÓN 3 — PROPUESTA DE VALOR
Usa: diferenciadores + producto_exclusivo de INFORMACIÓN DEL NEGOCIO
Articula la propuesta de valor única. Por qué este negocio y no la competencia. Conecta los diferenciadores con los problemas de la audiencia.

SECCIÓN 4 — ESTRATEGIA DE CANALES
Usa: INTELIGENCIA COMPETITIVA (síntesis cruzada — qué canales ignoran los competidores) + acuerdos de la reunión
Recomienda los canales de marketing prioritarios. Justifica cada canal basándote en: dónde está la audiencia, qué canales NO están saturados por la competencia (referencia los gaps detectados), y los acuerdos de la reunión.

SECCIÓN 5 — PLAN DE CONTENIDO
Usa: INTELIGENCIA DE KEYWORDS + INSIGHTS DE LA REUNIÓN
Propón una estrategia de contenido basada en las keywords investigadas, las preguntas frecuentes (people_also_ask) y la intención de búsqueda detectada. Prioriza contenido para keywords con intención transaccional o comercial. Incluye tipos de contenido, frecuencia, y temas prioritarios. Alinea con las metodologías discutidas en la reunión.

SECCIÓN 6 — ANÁLISIS COMPETITIVO
Usa: INTELIGENCIA COMPETITIVA (síntesis por competidor + patrones + oportunidades)
Presenta el análisis de cada competidor: sus estrategias de messaging, fortalezas y debilidades detectadas. Incluye las oportunidades de diferenciación. Este análisis debe ser estratégico (no una lista de anuncios).

SECCIÓN 7 — ESTRATEGIA DE KEYWORDS
Usa: INTELIGENCIA DE KEYWORDS (datos cuantitativos + cualitativos)
Presenta las keywords priorizadas con toda la inteligencia disponible:
- Si hay datos de volumen y CPC: incluir en formato tabla con keyword, volumen, competencia, CPC
- Si hay intención de búsqueda: agrupar keywords por intención (informational, transactional, commercial, navigational)
- Si hay interés relativo y tendencia: indicar keywords en crecimiento vs estacionales vs decrecientes
- Si hay análisis SERP: describir el panorama competitivo en búsqueda orgánica para las keywords principales
- Siempre incluir: keywords long-tail derivadas de búsquedas relacionadas y sugerencias de autocomplete
- Siempre incluir: oportunidades de contenido basadas en preguntas frecuentes (PAA)
La sección debe ser estratégica y accionable, no solo una tabla de números.

SECCIÓN 8 — KPIs Y MÉTRICAS
Usa: síntesis de TODAS las fuentes
Define KPIs medibles para cada canal y para el negocio en general. Los KPIs deben ser realistas para el tamaño del negocio y el presupuesto mencionado. Incluye metas a 30, 60 y 90 días.

SECCIÓN 9 — TIMELINE RECOMENDADO
Usa: síntesis de TODO + acuerdos_y_decisiones
Propón un timeline de implementación en semanas/meses. Las primeras acciones deben alinearse con los acuerdos de la reunión. Incluye hitos claros y responsables cuando sea posible.
"""


# 3B — Formatear el contexto como texto legible
def formatear_contexto(contexto: dict) -> str:
    """
    Convierte el diccionario de contexto en texto estructurado y legible
    que la IA pueda procesar eficientemente.
    v2.1: Genera narrativa adaptada según datos disponibles por keyword.
    """
    partes = []

    # --- Bloque 1: Información del Negocio ---
    negocio = contexto["informacion_del_negocio"]
    partes.append("=" * 60)
    partes.append("INFORMACIÓN DEL NEGOCIO")
    partes.append("=" * 60)
    partes.append(f"Propósito del negocio: {negocio['proposito']}")
    partes.append(f"Diferenciadores: {negocio['diferenciadores']}")
    partes.append(f"Producto exclusivo: {negocio['producto_exclusivo']}")
    partes.append(f"Precios promedio: {negocio['precios']}")
    partes.append(f"Ofertas especiales: {negocio['ofertas']}")
    partes.append(f"Público objetivo: {negocio['publico_objetivo']}")
    partes.append(f"Problemas que resuelve: {negocio['problemas_que_resuelve']}")
    partes.append(f"Competidores declarados: {', '.join(negocio['competidores_declarados'])}")
    partes.append(f"Errores de la competencia: {negocio['errores_competencia']}")

    # v2.1 — Mercado objetivo
    if negocio.get("mercado_objetivo"):
        partes.append(f"Mercado objetivo (país): {negocio['mercado_objetivo']}")

    # Productos — tratar como texto porque el formato no es limpio
    if negocio.get("productos_principales"):
        productos_texto = negocio["productos_principales"]
        if isinstance(productos_texto, list):
            productos_texto = " | ".join(str(p) for p in productos_texto)
        partes.append(f"Productos principales: {productos_texto}")

    if negocio.get("productos_a_promocionar"):
        promo_texto = negocio["productos_a_promocionar"]
        if isinstance(promo_texto, list):
            promo_texto = " | ".join(str(p) for p in promo_texto)
        partes.append(f"Productos a promocionar: {promo_texto}")

    # --- Bloque 2: Insights de la Reunión ---
    reunion = contexto["insights_de_la_reunion"]
    partes.append("")
    partes.append("=" * 60)
    partes.append("INSIGHTS DE LA REUNIÓN CON EL CLIENTE")
    partes.append("=" * 60)
    partes.append(f"Resumen ejecutivo: {reunion['resumen_ejecutivo']}")

    if reunion.get("objetivos_cliente"):
        partes.append("Objetivos del cliente:")
        for i, obj in enumerate(reunion["objetivos_cliente"], 1):
            partes.append(f"  {i}. {obj}")

    if reunion.get("metodologias_discutidas"):
        partes.append("Metodologías discutidas:")
        for m in reunion["metodologias_discutidas"]:
            partes.append(f"  - {m}")

    if reunion.get("acuerdos_y_decisiones"):
        partes.append("Acuerdos y decisiones tomadas:")
        for i, a in enumerate(reunion["acuerdos_y_decisiones"], 1):
            partes.append(f"  {i}. {a}")

    partes.append(f"Tono y personalidad de marca: {reunion['tono_y_personalidad']}")

    if reunion.get("keywords_mencionadas"):
        partes.append(f"Keywords mencionadas en la reunión: {', '.join(reunion['keywords_mencionadas'])}")

    # --- Bloque 3: Inteligencia Competitiva ---
    intel = contexto["inteligencia_competitiva"]
    partes.append("")
    partes.append("=" * 60)
    partes.append("INTELIGENCIA COMPETITIVA")
    partes.append("=" * 60)

    for comp in intel.get("competidores_analizados", []):
        partes.append(f"--- {comp['nombre']} ({comp['anuncios_analizados']} anuncios analizados) ---")
        if comp.get("sintesis"):
            partes.append(f"Síntesis: {comp['sintesis']}")
        if comp.get("patrones"):
            partes.append(f"Patrones de messaging: {comp['patrones']}")
        partes.append("")

    if intel.get("sintesis_cruzada"):
        partes.append("SÍNTESIS CRUZADA (oportunidades de mercado):")
        partes.append(intel["sintesis_cruzada"])

    # --- Bloque 4: Inteligencia de Keywords (v2.1) ---
    kw_data = contexto["inteligencia_de_keywords"]
    partes.append("")
    partes.append("=" * 60)
    partes.append("INTELIGENCIA DE KEYWORDS")
    partes.append("=" * 60)

    for kw in kw_data.get("keywords", []):
        # Línea principal con keyword y métricas disponibles
        linea = f"• {kw['keyword']}"
        metricas = []

        # Datos cuantitativos (KWE/KWP)
        if kw.get("volumen"):
            metricas.append(f"vol={kw['volumen']:,}")
        if kw.get("competencia"):
            metricas.append(f"comp={kw['competencia']}")
        if kw.get("cpc"):
            metricas.append(f"cpc=${kw['cpc']:.2f}")

        # Datos pytrends (v2.1)
        if kw.get("interes_relativo") is not None:
            metricas.append(f"interés={kw['interes_relativo']}/100")

        # Intención de búsqueda (v2.1)
        if kw.get("intencion_busqueda"):
            metricas.append(f"intención={kw['intencion_busqueda']}")

        # Presencia de anuncios pagados (proxy de competencia si no hay volumen)
        if kw.get("cantidad_anuncios_pagados", 0) > 0:
            metricas.append(f"anuncios_pagados={kw['cantidad_anuncios_pagados']}")

        if metricas:
            linea += f" [{', '.join(metricas)}]"
        partes.append(linea)

        # Análisis SERP IA (v2.1) — texto narrativo de Gemini
        if kw.get("analisis_serp_ia"):
            partes.append(f"    Análisis: {kw['analisis_serp_ia']}")

        # Tendencia 12m (v2.1) — solo si hay datos
        if kw.get("tendencia_12m") and len(kw["tendencia_12m"]) >= 2:
            tendencia = kw["tendencia_12m"]
            primer_val = tendencia[0] if isinstance(tendencia[0], (int, float)) else tendencia[0].get("valor", 0)
            ultimo_val = tendencia[-1] if isinstance(tendencia[-1], (int, float)) else tendencia[-1].get("valor", 0)
            if primer_val and ultimo_val:
                cambio = ((ultimo_val - primer_val) / primer_val * 100) if primer_val else 0
                direccion = "↑" if cambio > 5 else "↓" if cambio < -5 else "→"
                partes.append(f"    Tendencia 12m: {direccion} ({cambio:+.0f}%)")

        # Preguntas frecuentes (PAA)
        if kw.get("preguntas_frecuentes"):
            for q in kw["preguntas_frecuentes"][:2]:
                partes.append(f"    Pregunta frecuente: {q}")

        # Búsquedas relacionadas
        if kw.get("busquedas_relacionadas"):
            related = kw["busquedas_relacionadas"][:3]
            if related:
                partes.append(f"    Búsquedas relacionadas: {', '.join(str(r) for r in related)}")

    return "\n".join(partes)


def recortar_contexto(contexto_formateado: str, max_chars: int = 50000) -> str:
    """
    Si el contexto excede max_chars, recorta las secciones menos críticas.
    Gemini 2.5 Flash tiene 1M tokens — no lo necesita. Stub para modelos futuros.
    """
    if len(contexto_formateado) <= max_chars:
        return contexto_formateado
    # TODO: Implementar recorte progresivo si se usa un modelo con ventana pequeña
    return contexto_formateado[:max_chars]


# 3D — Ensamblar el prompt final
def ensamblar_prompt(contexto: dict) -> dict:
    """
    Retorna el prompt completo como dict con las 3 partes separadas.
    Esto permite que Etapa 5 lo pase al AIProvider de la forma que necesite.
    """
    contexto_formateado = formatear_contexto(contexto)
    contexto_formateado = recortar_contexto(contexto_formateado)

    return {
        "system_prompt": SYSTEM_PROMPT,
        "contexto_formateado": contexto_formateado,
        "instrucciones": INSTRUCCIONES_SECCIONES,
        # Prompt combinado (para guardar en DB y para modelos sin system prompt separado)
        "prompt_combinado": f"{SYSTEM_PROMPT}\n\n{contexto_formateado}\n\n{INSTRUCCIONES_SECCIONES}",
    }


# =====================================================================
# Paso 4 — Persistir en metodologias_generadas
# =====================================================================

async def _persistir_prompt(cliente_id: str, prompt_combinado: str, db) -> str:
    """
    Guarda el prompt en metodologias_generadas y retorna el UUID del registro.
    ia_utilizada queda NULL — la Etapa 5 lo actualizará cuando genere.
    intentos_generacion en 0 — la Etapa 5 lo incrementará.
    """
    resultado = await db.guardar_metodologia(cliente_id, {
        "prompt_utilizado": prompt_combinado,
        "ia_utilizada": None,
        "intentos_generacion": 0,
        "secciones_completadas": [],
        "estado_revision": "pendiente",
        "contenido_generado": None,
        "documento_drive_id": None,
    })

    metodologia_id = str(resultado["id"]) if resultado else None
    return metodologia_id


# =====================================================================
# Función principal
# =====================================================================

async def consolidar(cliente_id: str, db, cache=None) -> dict:
    """
    Consulta las 4 tablas fuente y construye el contexto consolidado + prompt.

    Args:
        cliente_id: UUID del cliente
        db: instancia del cliente de base de datos (db/client.py)
        cache: instancia del cliente Redis (opcional — para actualizar estado)

    Returns:
        dict con keys:
            - contexto: dict con los 4 bloques temáticos
            - prompt_completo: dict con system_prompt, contexto_formateado, instrucciones
            - metodologia_id: UUID del registro creado en metodologias_generadas
    """
    logger.info(f"cliente={cliente_id} — Iniciando consolidación")

    # Paso 1 — Obtener datos de las 4 tablas fuente
    fuentes = await _obtener_datos_fuente(cliente_id, db)

    # Paso 2 — Estructurar en bloques temáticos
    contexto = _estructurar_bloques(fuentes)

    # Log de cobertura de keywords (v2.1)
    keywords_rows = fuentes["keywords_rows"]
    if keywords_rows:
        kw_con_volumen = sum(1 for kw in keywords_rows if kw.get("volumen_busqueda"))
        kw_con_intencion = sum(1 for kw in keywords_rows if kw.get("intencion_busqueda"))
        kw_con_interes = sum(1 for kw in keywords_rows if kw.get("interes_relativo") is not None)
        logger.info(
            f"cliente={cliente_id} — Cobertura keywords: "
            f"{kw_con_volumen}/{len(keywords_rows)} con volumen, "
            f"{kw_con_intencion}/{len(keywords_rows)} con intención, "
            f"{kw_con_interes}/{len(keywords_rows)} con interés relativo"
        )

    # Paso 3 — Construir prompt
    prompt_dict = ensamblar_prompt(contexto)
    contexto_formateado = prompt_dict["contexto_formateado"]
    logger.info(
        f"cliente={cliente_id} — Contexto formateado: "
        f"{len(contexto_formateado)} chars (~{len(contexto_formateado) // 4} tokens)"
    )

    # Paso 4 — Persistir en DB
    metodologia_id = await _persistir_prompt(
        cliente_id, prompt_dict["prompt_combinado"], db
    )
    logger.info(f"cliente={cliente_id} — Prompt guardado en metodologias_generadas id={metodologia_id}")

    logger.info(f"cliente={cliente_id} — Consolidación completada")

    # Paso 5 — Retorno
    return {
        "contexto": contexto,
        "prompt_completo": prompt_dict,
        "metodologia_id": metodologia_id,
    }
