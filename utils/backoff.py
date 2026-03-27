"""
utils/backoff.py — Retry con backoff exponencial para llamadas async.
"""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def retry_with_backoff(
    func,
    max_retries: int = 3,
    base_delay: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Any:
    """
    Ejecuta func() con reintentos y backoff exponencial.
    Progresión: base_delay * 2^intento (2s → 4s → 8s con base_delay=2).
    Si se agotan los reintentos, re-lanza la última excepción.
    """
    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            return await func()
        except exceptions as e:
            last_exception = e
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    f"Intento {attempt + 1}/{max_retries + 1} falló: {e}. "
                    f"Reintentando en {delay}s..."
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"Todos los {max_retries + 1} intentos fallaron. Última excepción: {e}"
                )
    raise last_exception
