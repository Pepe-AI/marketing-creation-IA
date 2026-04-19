"""
services/methodology.py — Etapa 5: Generación de metodología de marketing con IA.

Toma el contexto consolidado de Etapa 4 y genera el documento de metodología
usando AIProvider (Gemini por defecto). Valida que las 9 secciones estén presentes
y reintenta si faltan (máximo 3 intentos).
"""

import os
import json
import logging

from ai.provider import AIProvider
from utils.telegram import enviar_telegram

logger = logging.getLogger(__name__)

MAX_INTENTOS = 3

# =====================================================================
# Secciones requeridas y validación
# =====================================================================

SECCIONES_REQUERIDAS = [
    "SECCIÓN 1: ANÁLISIS DE SITUACIÓN",
    "SECCIÓN 2: AUDIENCIA OBJETIVO",
    "SECCIÓN 3: PROPUESTA DE VALOR",
    "SECCIÓN 4: ESTRATEGIA DE CANALES",
    "SECCIÓN 5: PLAN DE CONTENIDO",
    "SECCIÓN 6: ANÁLISIS COMPETITIVO",
    "SECCIÓN 7: ESTRATEGIA DE KEYWORDS",
    "SECCIÓN 8: KPIS Y MÉTRICAS",
    "SECCIÓN 9: TIMELINE RECOMENDADO",
]


def validar_secciones(texto: str) -> dict:
    """
    Verifica qué secciones están presentes en el texto generado.
    Búsqueda flexible: con/sin ##, con/sin acentos parciales.
    """
    texto_upper = texto.upper()
    presentes = []
    faltantes = []

    for seccion in SECCIONES_REQUERIDAS:
        # Extraer nombre clave (ej: "ANÁLISIS DE SITUACIÓN")
        nombre_clave = seccion.split(": ", 1)[1] if ": " in seccion else seccion
        if nombre_clave.upper() in texto_upper or seccion.upper() in texto_upper:
            presentes.append(seccion)
        else:
            faltantes.append(seccion)

    return {
        "completo": len(faltantes) == 0,
        "secciones_presentes": presentes,
        "secciones_faltantes": faltantes,
        "total_encontradas": len(presentes),
    }


def _build_retry_instruction(secciones_faltantes: list, texto_anterior: str) -> str:
    """Genera instrucción de reintento indicando qué secciones faltan."""
    secciones_str = "\n".join(f"- {s}" for s in secciones_faltantes)
    return (
        f"El texto generado anteriormente está INCOMPLETO. Faltan estas secciones:\n\n"
        f"{secciones_str}\n\n"
        f"Aquí está el texto que generaste hasta ahora:\n---\n{texto_anterior}\n---\n\n"
        f"Ahora genera ÚNICAMENTE las secciones faltantes indicadas arriba, usando los "
        f"mismos encabezados exactos. No repitas las secciones que ya están completas."
    )


# =====================================================================
# System prompt y template de 9 secciones
# =====================================================================

SYSTEM_PROMPT = """Eres un consultor senior de marketing digital especializado en estrategias para negocios latinoamericanos. Tu tarea es crear una metodología de marketing completa, profesional y accionable basada en los datos reales del cliente que se te proporcionan.

REGLAS:
- Usa ÚNICAMENTE los datos proporcionados. No inventes información.
- Sé específico: menciona nombres de productos, competidores, keywords reales.
- Cada sección debe tener entre 150 y 400 palabras.
- Usa lenguaje profesional pero accesible (no jerga excesiva).
- Estructura cada sección con el encabezado exacto indicado.
- El documento final debe ser listo para entregar al cliente sin edición mayor.
"""

INSTRUCCION_GENERACION = """Genera una metodología de marketing completa con EXACTAMENTE estas 9 secciones, usando los encabezados exactos indicados:

## SECCIÓN 1: ANÁLISIS DE SITUACIÓN
Describe el estado actual del negocio basándote en el propósito, diferenciadores, productos, precios y mercado. Incluye el contexto competitivo general.
Fuente: informacion_del_negocio + inteligencia_competitiva

## SECCIÓN 2: AUDIENCIA OBJETIVO
Define el perfil del cliente ideal con detalle: demografía, psicografía, problemas que enfrenta y cómo el negocio los resuelve.
Fuente: datos_form.publico_objetivo + datos_form.problemas_que_resuelve

## SECCIÓN 3: PROPUESTA DE VALOR
Articula la propuesta de valor única del negocio. ¿Por qué un cliente debería elegirlos sobre la competencia? Basado en diferenciadores y producto exclusivo.
Fuente: datos_form.diferenciadores + datos_form.producto_exclusivo

## SECCIÓN 4: ESTRATEGIA DE CANALES
Recomienda los canales de marketing prioritarios basándote en los canales que la competencia está usando (y cuáles están ignorando). Incluye justificación.
Fuente: inteligencia_competitiva (sintesis_competitiva)

## SECCIÓN 5: PLAN DE CONTENIDO
Define los tipos de contenido, temas prioritarios y frecuencia recomendada. Basa los temas en las keywords con mayor intención transaccional y los insights de la reunión.
Fuente: inteligencia_de_keywords + insights_de_la_reunion

## SECCIÓN 6: ANÁLISIS COMPETITIVO
Describe el landscape competitivo: qué están haciendo los competidores, sus fortalezas, debilidades detectadas y las oportunidades de diferenciación.
Fuente: inteligencia_competitiva (patrones, oportunidades, sintesis)

## SECCIÓN 7: ESTRATEGIA DE KEYWORDS
Define la estrategia de keywords organizada por intención de búsqueda (informacional, transaccional, comercial). Incluye nivel de demanda, tendencia estacional cuando disponible, y oportunidades de posicionamiento.
Fuente: inteligencia_de_keywords (intencion_busqueda, analisis_serp_ia, interes_relativo, tendencia_12m)

## SECCIÓN 8: KPIS Y MÉTRICAS
Define 5-8 KPIs específicos y medibles, con valores de referencia realistas para el primer trimestre. Incluye cómo medirlos.
Fuente: síntesis de todas las fuentes

## SECCIÓN 9: TIMELINE RECOMENDADO
Plan de ejecución de 90 días dividido en 3 fases de 30 días cada una. Incluye los acuerdos y decisiones de la reunión como puntos de acción concretos.
Fuente: todas las fuentes + insights_de_la_reunion.acuerdos_decisiones

---
IMPORTANTE: Incluye los 9 encabezados exactamente como están escritos arriba.
Cada sección debe estar claramente delimitada. El documento debe fluir de forma coherente y profesional.
"""


# =====================================================================
# Función principal
# =====================================================================

async def generar_metodologia(cliente_id: str, contexto: dict, db=None) -> str:
    """
    Etapa 5: Genera la metodología de marketing con IA.

    Args:
        cliente_id: UUID del cliente
        contexto: dict retornado por consolidar() con keys:
            - contexto: dict con los 4 bloques temáticos
            - prompt_completo: dict con system_prompt, contexto_formateado, instrucciones
            - metodologia_id: UUID del registro en metodologias_generadas

    Returns:
        str: texto completo de la metodología generada
    """
    logger.info(f"cliente={cliente_id} — Iniciando generación de metodología")

    # Extraer el contexto formateado de Etapa 4
    prompt_data = contexto.get("prompt_completo", {})
    contexto_formateado = prompt_data.get("contexto_formateado", "")
    metodologia_id = contexto.get("metodologia_id")

    if not contexto_formateado:
        logger.error(f"cliente={cliente_id} — Sin contexto formateado, no se puede generar")
        return ""

    # Inicializar provider de IA
    ai = AIProvider()
    ia_utilizada = os.environ.get("AI_PROVIDER", "gemini").lower()

    intentos = 0
    texto_generado = ""
    validacion = {"completo": False, "secciones_presentes": [], "secciones_faltantes": SECCIONES_REQUERIDAS}
    instruccion_actual = INSTRUCCION_GENERACION

    # Loop de generación con reintentos
    while intentos < MAX_INTENTOS:
        intentos += 1
        logger.info(f"cliente={cliente_id} — Intento {intentos}/{MAX_INTENTOS}")

        try:
            fragmento = await ai.generate(
                system_prompt=SYSTEM_PROMPT,
                context=contexto_formateado,
                instruction=instruccion_actual,
                timeout=120,  # Generación larga — dar 2 minutos
                max_output_tokens=8192,  # ~5000 palabras para la metodología completa
            )

            # En reintentos: concatenar secciones faltantes, no reemplazar
            if intentos == 1:
                texto_generado = fragmento
            else:
                texto_generado = texto_generado + "\n\n" + fragmento

            validacion = validar_secciones(texto_generado)
            logger.info(
                f"cliente={cliente_id} — Intento {intentos}: "
                f"{validacion['total_encontradas']}/9 secciones encontradas"
            )

            if validacion["completo"]:
                logger.info(f"cliente={cliente_id} — Metodología completa en intento {intentos}")
                break

            # Preparar instrucción de reintento
            instruccion_actual = _build_retry_instruction(
                validacion["secciones_faltantes"], texto_generado
            )

        except Exception as e:
            logger.error(f"cliente={cliente_id} — Error en intento {intentos}: {e}")
            if intentos >= MAX_INTENTOS:
                raise

    # Guardar resultado en DB
    if db and metodologia_id:
        try:
            await db.guardar_metodologia(cliente_id, {
                "contenido_generado": texto_generado,
                "ia_utilizada": ia_utilizada,
                "intentos_generacion": intentos,
                "secciones_completadas": validacion["secciones_presentes"],
                "estado_revision": "pendiente",
                "prompt_utilizado": prompt_data.get("prompt_combinado", ""),
            })
            logger.info(
                f"cliente={cliente_id} — Metodología guardada: "
                f"{len(texto_generado)} chars, {validacion['total_encontradas']}/9 secciones"
            )
        except Exception as e:
            logger.error(f"cliente={cliente_id} — Error guardando metodología en DB: {e}")

    # Notificar si salió incompleta
    if not validacion["completo"]:
        faltantes_str = ", ".join(validacion["secciones_faltantes"])
        try:
            await enviar_telegram(
                f"⚠️ Metodología incompleta — {intentos} intentos\n"
                f"Cliente: <code>{cliente_id}</code>\n"
                f"Secciones faltantes: {faltantes_str}\n"
                f"Proveedor IA: {ia_utilizada}\n"
                f"Se guardó el documento parcial en DB."
            )
        except Exception:
            logger.warning("No se pudo enviar notificación Telegram de metodología incompleta")

    logger.info(f"cliente={cliente_id} — Generación de metodología completada")
    return texto_generado
