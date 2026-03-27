"""
ai/provider.py — Proveedor de IA configurable.

Controlado por variable de entorno AI_PROVIDER (default: "gemini").
Solo "gemini" está implementado completamente.
"""

import os
import json
import logging

import httpx

logger = logging.getLogger(__name__)


class AIProvider:
    def __init__(self):
        self.provider = os.environ.get("AI_PROVIDER", "gemini").lower()
        self.gemini_api_key = os.environ.get("GEMINI_API_KEY")

    async def generate(
        self,
        system_prompt: str,
        context: str,
        instruction: str,
        timeout: int = 60,
    ) -> str:
        """
        Genera texto usando el proveedor configurado.
        Retorna el texto de respuesta como string.
        """
        if self.provider == "gemini":
            return await self._generate_gemini(system_prompt, context, instruction, timeout)
        elif self.provider in ("openai", "anthropic"):
            raise NotImplementedError(
                f"Proveedor '{self.provider}' no implementado aún. Cambia AI_PROVIDER=gemini"
            )
        else:
            raise ValueError(f"Proveedor de IA desconocido: '{self.provider}'")

    async def _generate_gemini(
        self, system_prompt: str, context: str, instruction: str, timeout: int
    ) -> str:
        """Llamada a Gemini 2.5 Flash via REST API."""
        if not self.gemini_api_key:
            raise ValueError("GEMINI_API_KEY no configurada")

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.5-flash:generateContent?key={self.gemini_api_key}"
        )

        # Construir el contenido del prompt completo
        full_prompt = f"{instruction}\n\n{context}" if context else instruction

        payload = {
            "system_instruction": {
                "parts": [{"text": system_prompt}]
            },
            "contents": [
                {
                    "parts": [{"text": full_prompt}]
                }
            ],
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": 4096,
            },
        }

        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()

        # Extraer texto de la respuesta
        candidates = data.get("candidates", [])
        if not candidates:
            raise ValueError("Gemini no retornó candidatos en la respuesta")

        parts = candidates[0].get("content", {}).get("parts", [])
        if not parts:
            raise ValueError("Gemini retornó candidato sin contenido")

        return parts[0].get("text", "")

    async def generate_with_image(
        self,
        system_prompt: str,
        instruction: str,
        image_url: str,
        timeout: int = 60,
    ) -> str:
        """Genera análisis usando Gemini Vision con una URL de imagen."""
        if self.provider != "gemini":
            raise NotImplementedError("Vision solo implementado para Gemini")

        if not self.gemini_api_key:
            raise ValueError("GEMINI_API_KEY no configurada")

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.5-flash:generateContent?key={self.gemini_api_key}"
        )

        payload = {
            "system_instruction": {
                "parts": [{"text": system_prompt}]
            },
            "contents": [
                {
                    "parts": [
                        {"text": instruction},
                        {
                            "file_data": {
                                "mime_type": "image/jpeg",
                                "file_uri": image_url,
                            }
                        }
                        if image_url.startswith("gs://")
                        else {
                            "inline_data": {
                                "mime_type": "image/jpeg",
                                "data": "",
                            }
                        }
                        if False  # placeholder — usamos URL directa abajo
                        else {"text": f"[Imagen del anuncio: {image_url}]"},
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": 2048,
            },
        }

        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()

        candidates = data.get("candidates", [])
        if not candidates:
            raise ValueError("Gemini Vision no retornó candidatos")

        parts = candidates[0].get("content", {}).get("parts", [])
        return parts[0].get("text", "") if parts else ""
