"""
utils/telegram.py — Notificaciones via Telegram Bot API.
"""

import os
import logging

import httpx

logger = logging.getLogger(__name__)


async def enviar_telegram(mensaje: str, chat_id: str = None, bot_token: str = None) -> bool:
    """
    Envía un mensaje de texto via Telegram Bot API.
    Nunca lanza excepción — loggea errores y retorna False.
    """
    bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.warning("TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID no configurados")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": mensaje,
        "parse_mode": "HTML",
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code == 200:
                logger.info("Notificación Telegram enviada correctamente")
                return True
            else:
                logger.warning(f"Telegram API respondió {resp.status_code}: {resp.text}")
                return False
    except Exception as e:
        logger.error(f"Error enviando Telegram: {e}")
        return False
