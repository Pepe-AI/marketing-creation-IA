"""
services/transcript.py — Etapa 3.1: Procesamiento de transcript con Gemini.
"""

import json
import logging

from ai.provider import AIProvider
from utils.backoff import retry_with_backoff

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Eres un analista de marketing experto. Analiza transcripts de reuniones de consultoría. "
    "Responde ÚNICAMENTE con JSON válido, sin markdown, sin bloques de código, sin texto extra."
)

INSTRUCTION_TEMPLATE = """Analiza el siguiente transcript y extrae exactamente este JSON:
{{
  "resumen_ejecutivo": "párrafo de 3-5 oraciones resumiendo la reunión",
  "objetivos_cliente": ["objetivo1", "objetivo2"],
  "keywords_mencionadas": ["keyword1", "keyword2"],
  "metodologias_discutidas": ["metodologia1"],
  "acuerdos_decisiones": ["acuerdo1"],
  "tono_personalidad": "descripción del tono y personalidad de marca detectados"
}}

Límite keywords_mencionadas: máximo 15 keywords de negocio relevantes.

Transcript:
{transcript_raw}"""


async def procesar_transcript(cliente_id: str, transcript_raw: str) -> dict:
    """
    Analiza el transcript con Gemini y retorna el análisis estructurado.
    PASO CRÍTICO: si falla tras reintentos, lanza excepción que detiene el pipeline.
    """
    logger.info(f"Iniciando análisis de transcript para cliente={cliente_id}")

    ai = AIProvider()
    instruction = INSTRUCTION_TEMPLATE.format(transcript_raw=transcript_raw)

    async def _call_gemini():
        response_text = await ai.generate(
            system_prompt=SYSTEM_PROMPT,
            context="",
            instruction=instruction,
            timeout=60,
        )
        # Limpiar posibles bloques de código markdown
        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            # Remover ```json y ``` del final
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines)

        return json.loads(cleaned)

    # Max 2 reintentos (3 intentos totales) con backoff 2s → 4s
    analisis = await retry_with_backoff(
        func=_call_gemini,
        max_retries=2,
        base_delay=2.0,
        exceptions=(Exception,),
    )

    logger.info(f"Transcript analizado exitosamente para cliente={cliente_id}")
    return analisis
