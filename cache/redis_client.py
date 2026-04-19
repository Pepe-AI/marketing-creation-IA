"""
cache/redis_client.py — Capa de caché y estado del pipeline (async).

Gestiona:
1. Caché de APIs externas (Apify, SerpAPI, Keyword Planner, Gemini Vision) con TTLs específicos
2. Caché de resolución de URLs de Facebook por nombre de competidor
3. Estado del pipeline en tiempo real con TTL de 24 horas
"""

import os
import json
import hashlib
import logging
from typing import Optional
from datetime import datetime, timezone

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# TTLs
TTL_CACHE_APIS = 60 * 60 * 24 * 7       # 7 días (604800 segundos)
TTL_PIPELINE_STATUS = 60 * 60 * 24      # 24 horas (86400 segundos)
TTL_FB_URL = 60 * 60 * 24 * 30          # 30 días (2592000 segundos)
TTL_GEMINI_AD = 60 * 60 * 24 * 30       # 30 días (2592000 segundos)


class RedisClient:
    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url or os.environ.get("REDIS_URL")
        if not self.redis_url:
            raise ValueError("REDIS_URL no configurada")

        self.client = aioredis.from_url(
            self.redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )

    async def ping(self) -> bool:
        """Verifica la conexión a Redis."""
        try:
            return await self.client.ping()
        except aioredis.ConnectionError:
            return False

    async def close(self):
        """Cierra la conexión a Redis."""
        await self.client.aclose()

    # =================================================================
    # UTILIDADES
    # =================================================================

    @staticmethod
    def _hash_key(value: str) -> str:
        """Genera un hash MD5 completo para usar como parte de la key."""
        return hashlib.md5(value.strip().lower().encode()).hexdigest()

    async def _set_json(self, key: str, data: dict, ttl: int) -> bool:
        """Guarda un dict como JSON con TTL."""
        try:
            await self.client.setex(key, ttl, json.dumps(data, ensure_ascii=False, default=str))
            return True
        except aioredis.RedisError as e:
            logger.warning(f"Redis SET error para key={key}: {e}")
            return False

    async def _get_json(self, key: str) -> Optional[dict]:
        """Lee una key y la parsea como JSON. Retorna None si no existe o expiró."""
        try:
            value = await self.client.get(key)
            if value is None:
                return None
            return json.loads(value)
        except (aioredis.RedisError, json.JSONDecodeError) as e:
            logger.warning(f"Redis GET error para key={key}: {e}")
            return None

    # =================================================================
    # CACHÉ: META AD LIBRARY (DEPRECATED — usar Apify en su lugar)
    # =================================================================

    def _meta_key(self, competidor: str) -> str:
        return f"meta_ads:{self._hash_key(competidor)}"

    # DEPRECATED: set_meta_ads y get_meta_ads ya no se usan.
    # Se reemplazaron por set_apify_ads / get_apify_ads en Etapa 3.
    async def set_meta_ads(self, competidor: str, datos: dict) -> bool:
        """DEPRECATED — Usar set_apify_ads en su lugar."""
        payload = {
            "competidor": competidor,
            "datos": datos,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }
        return await self._set_json(self._meta_key(competidor), payload, TTL_CACHE_APIS)

    async def get_meta_ads(self, competidor: str) -> Optional[dict]:
        """DEPRECATED — Usar get_apify_ads en su lugar."""
        result = await self._get_json(self._meta_key(competidor))
        return result["datos"] if result else None

    # =================================================================
    # CACHÉ: URL DE FACEBOOK POR NOMBRE DE COMPETIDOR
    # Key pattern: fb_url:{md5(nombre.lower())}
    # TTL: 30 días
    # =================================================================

    def _fb_url_key(self, nombre_competidor: str) -> str:
        return f"fb_url:{self._hash_key(nombre_competidor)}"

    async def set_fb_url(self, nombre_competidor: str, url: str) -> bool:
        """Cachea la URL de Facebook resuelta para un nombre de competidor."""
        payload = {
            "nombre": nombre_competidor,
            "url": url,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }
        return await self._set_json(self._fb_url_key(nombre_competidor), payload, TTL_FB_URL)

    async def get_fb_url(self, nombre_competidor: str) -> Optional[str]:
        """Obtiene la URL de Facebook cacheada para un competidor. None si no existe o expiró."""
        result = await self._get_json(self._fb_url_key(nombre_competidor))
        return result["url"] if result else None

    # =================================================================
    # CACHÉ: APIFY ADS (scraping completo por URL de Facebook)
    # Key pattern: apify:{md5(facebook_url)}
    # TTL: 7 días
    # =================================================================

    def _apify_key(self, facebook_url: str) -> str:
        return f"apify:{hashlib.md5(facebook_url.encode()).hexdigest()}"

    async def set_apify_ads(self, facebook_url: str, datos: dict) -> bool:
        """Cachea el dataset completo de Apify para una URL de Facebook."""
        payload = {
            "facebook_url": facebook_url,
            "datos": datos,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }
        return await self._set_json(self._apify_key(facebook_url), payload, TTL_CACHE_APIS)

    async def get_apify_ads(self, facebook_url: str) -> Optional[dict]:
        """Obtiene datos cacheados de Apify. None si no existe o expiró."""
        result = await self._get_json(self._apify_key(facebook_url))
        return result["datos"] if result else None

    # =================================================================
    # CACHÉ: GEMINI VISION — ANÁLISIS POR ANUNCIO INDIVIDUAL
    # Key pattern: gemini_ad:{md5(creative_url)}
    # TTL: 30 días
    # =================================================================

    def _gemini_ad_key(self, creative_url: str) -> str:
        return f"gemini_ad:{hashlib.md5(creative_url.encode()).hexdigest()}"

    async def set_gemini_ad_analysis(self, creative_url: str, datos: dict) -> bool:
        """Cachea el análisis Gemini Vision de un creativo individual."""
        payload = {
            "creative_url": creative_url,
            "datos": datos,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }
        return await self._set_json(self._gemini_ad_key(creative_url), payload, TTL_GEMINI_AD)

    async def get_gemini_ad_analysis(self, creative_url: str) -> Optional[dict]:
        """Obtiene análisis Gemini cacheado. None si no existe o expiró."""
        result = await self._get_json(self._gemini_ad_key(creative_url))
        return result["datos"] if result else None

    # =================================================================
    # CACHÉ: SERPAPI
    # Key pattern: serp:{md5(keyword.lower())}
    # TTL: 7 días
    # =================================================================

    def _serp_key(self, keyword: str) -> str:
        return f"serp:{self._hash_key(keyword)}"

    async def set_serp(self, keyword: str, datos: dict) -> bool:
        """Cachea los resultados de SerpAPI para una keyword."""
        payload = {
            "keyword": keyword,
            "datos": datos,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }
        return await self._set_json(self._serp_key(keyword), payload, TTL_CACHE_APIS)

    async def get_serp(self, keyword: str) -> Optional[dict]:
        """Obtiene datos cacheados de SerpAPI. None si no existe o expiró."""
        result = await self._get_json(self._serp_key(keyword))
        return result["datos"] if result else None

    # =================================================================
    # CACHÉ: GOOGLE KEYWORD PLANNER
    # Key pattern: kwp:{md5(keyword.lower())}
    # TTL: 7 días
    # =================================================================

    def _kwp_key(self, keyword: str) -> str:
        return f"kwp:{self._hash_key(keyword)}"

    async def set_keyword_planner(self, keyword: str, datos: dict) -> bool:
        """Cachea los resultados de Google Keyword Planner."""
        payload = {
            "keyword": keyword,
            "datos": datos,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }
        return await self._set_json(self._kwp_key(keyword), payload, TTL_CACHE_APIS)

    async def get_keyword_planner(self, keyword: str) -> Optional[dict]:
        """Obtiene datos cacheados de Keyword Planner. None si no existe o expiró."""
        result = await self._get_json(self._kwp_key(keyword))
        return result["datos"] if result else None

    # =================================================================
    # ESTADO DEL PIPELINE
    # Key pattern: pipeline:{cliente_id}:status
    # TTL: 24 horas
    # =================================================================

    def _pipeline_key(self, cliente_id: str) -> str:
        return f"pipeline:{cliente_id}:status"

    async def set_pipeline_status(
        self,
        cliente_id: str,
        estado: str,
        paso: int = 0,
        detalle: str = None,
        error: str = None,
    ) -> bool:
        payload = {
            "cliente_id": cliente_id,
            "estado": estado,
            "paso": paso,
            "detalle": detalle,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return await self._set_json(self._pipeline_key(cliente_id), payload, TTL_PIPELINE_STATUS)

    async def get_pipeline_status(self, cliente_id: str) -> Optional[dict]:
        """Obtiene el estado actual del pipeline de un cliente."""
        return await self._get_json(self._pipeline_key(cliente_id))

    async def clear_pipeline_status(self, cliente_id: str) -> bool:
        """Limpia el estado del pipeline."""
        try:
            await self.client.delete(self._pipeline_key(cliente_id))
            return True
        except aioredis.RedisError:
            return False

    # =================================================================
    # UTILIDADES DE DIAGNÓSTICO
    # =================================================================

    async def get_info(self) -> dict:
        """Información general de Redis para diagnóstico."""
        try:
            info = await self.client.info()
            return {
                "connected": True,
                "version": info.get("redis_version", "unknown"),
                "used_memory_human": info.get("used_memory_human", "unknown"),
                "connected_clients": info.get("connected_clients", 0),
                "total_keys": await self.client.dbsize(),
            }
        except aioredis.RedisError as e:
            return {"connected": False, "error": str(e)}

    async def list_keys(self, pattern: str = "*") -> list[str]:
        """Lista keys que coincidan con un patrón. Solo para debugging."""
        try:
            return await self.client.keys(pattern)
        except aioredis.RedisError:
            return []

    # =================================================================
    # CACHÉ: PYTRENDS (Google Trends por keyword)
    # Key pattern: pytrends:{md5(keyword.lower())}
    # TTL: 7 días
    # =================================================================

    def _pytrends_key(self, keyword: str) -> str:
        return f"pytrends:{self._hash_key(keyword)}"

    async def set_pytrends(self, keyword: str, datos: dict) -> bool:
        """Cachea los datos de pytrends para una keyword (interés relativo + tendencia 12m)."""
        payload = {
            "keyword": keyword,
            "datos": datos,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }
        return await self._set_json(self._pytrends_key(keyword), payload, TTL_CACHE_APIS)

    async def get_pytrends(self, keyword: str) -> Optional[dict]:
        """Obtiene datos cacheados de pytrends. None si no existe o expiró."""
        result = await self._get_json(self._pytrends_key(keyword))
        return result["datos"] if result else None

    # =================================================================
    # CACHÉ: GEMINI KEYWORD ANALYSIS (batch por cliente)
    # Key pattern: gemini_kw_analysis:{cliente_id}
    # TTL: 7 días
    # =================================================================

    def _gemini_kw_key(self, cliente_id: str) -> str:
        return f"gemini_kw_analysis:{cliente_id}"

    async def set_gemini_kw_analysis(self, cliente_id: str, datos: dict) -> bool:
        """Cachea el análisis batch de Gemini de todas las keywords de un cliente."""
        payload = {
            "cliente_id": cliente_id,
            "datos": datos,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }
        return await self._set_json(self._gemini_kw_key(cliente_id), payload, TTL_CACHE_APIS)

    async def get_gemini_kw_analysis(self, cliente_id: str) -> Optional[dict]:
        """Obtiene análisis de keywords cacheado. None si no existe o expiró."""
        result = await self._get_json(self._gemini_kw_key(cliente_id))
        return result["datos"] if result else None

    async def flush_cache(self, pattern: str = None) -> int:
        """Limpia keys de caché."""
        try:
            if pattern is None:
                await self.client.flushdb()
                return -1
            else:
                keys = await self.client.keys(pattern)
                if keys:
                    return await self.client.delete(*keys)
                return 0
        except aioredis.RedisError:
            return 0
