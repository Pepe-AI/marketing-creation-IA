"""
main.py — Microservicio ecosfera-pipeline (FastAPI).

Endpoint principal: POST /procesar-transcript
Recibe { "cliente_id": "uuid" } y ejecuta el pipeline completo en background.
"""

import os
import json
import asyncio
import logging
import traceback
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

from db.client import DatabaseClient
from cache.redis_client import RedisClient
from services.transcript import procesar_transcript
from services.competitors import analizar_competidores
from services.keywords import researchar_keywords
from services.consolidator import consolidar
from services.methodology import generar_metodologia
from services.document import crear_documento
from utils.telegram import enviar_telegram

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

class Settings(BaseSettings):
    DATABASE_URL: str
    REDIS_URL: str
    GEMINI_API_KEY: str
    SERPAPI_KEY: str = ""
    APIFY_API_TOKEN: str = ""
    APIFY_ACTOR_ID: str = "curious_coder/facebook-ads-library-scraper"
    GOOGLE_CREDENTIALS_JSON: str = ""
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = "5196101763"
    AI_PROVIDER: str = "gemini"

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

# Corregir DATABASE_URL si tiene prefijo jdbc:
db_url = settings.DATABASE_URL
if db_url.startswith("jdbc:"):
    db_url = db_url[5:]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecosfera-pipeline")

# ---------------------------------------------------------------------------
# Clientes globales
# ---------------------------------------------------------------------------

db = DatabaseClient(db_url)
cache = RedisClient(settings.REDIS_URL)

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    logger.info("Servicio ecosfera-pipeline iniciado")
    yield
    await db.close()
    await cache.close()
    logger.info("Servicio ecosfera-pipeline detenido")

app = FastAPI(
    title="ecosfera-pipeline",
    version="1.0.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Modelos
# ---------------------------------------------------------------------------

class ProcesarRequest(BaseModel):
    cliente_id: str

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok", "service": "ecosfera-pipeline", "version": "1.0.0"}


@app.get("/health/db")
async def health_db():
    """Verifica conexión a PostgreSQL y lista las tablas públicas."""
    async with db._pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
        rows = await conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
    return {"db": "ok", "tables": [r["table_name"] for r in rows]}


@app.get("/pipeline/{cliente_id}")
async def pipeline_status(cliente_id: str):
    """Consulta el estado del pipeline de un cliente."""
    cliente = await db.obtener_cliente(cliente_id)
    if not cliente:
        raise HTTPException(status_code=404, detail=f"Cliente {cliente_id} no encontrado")

    estado_redis = await cache.get_pipeline_status(cliente_id)
    return {
        "cliente_id": cliente_id,
        "estado_db": cliente.get("estado_pipeline"),
        "estado_redis": estado_redis,
        "ultima_actualizacion": cliente.get("fecha_actualizacion"),
    }


@app.post("/procesar-transcript", status_code=202)
async def procesar_transcript_endpoint(
    request: ProcesarRequest,
    background_tasks: BackgroundTasks,
):
    """
    Inicia el pipeline de procesamiento en background.
    Retorna 202 Accepted inmediatamente.
    """
    cliente_id = request.cliente_id

    # Verificar que el cliente existe
    cliente = await db.obtener_cliente(cliente_id)
    if not cliente:
        raise HTTPException(status_code=404, detail=f"Cliente {cliente_id} no encontrado")

    # Lanzar procesamiento en background
    background_tasks.add_task(ejecutar_pipeline, cliente_id)

    return {
        "status": "procesando",
        "cliente_id": cliente_id,
        "message": "Pipeline iniciado",
    }


# ---------------------------------------------------------------------------
# Pipeline Background Task
# ---------------------------------------------------------------------------

async def ejecutar_pipeline(cliente_id: str):
    """
    Pipeline completo de procesamiento. Corre como background task.
    try/except global garantiza siempre: Telegram, DB y Redis actualizados.
    """
    nombre_cliente = "desconocido"
    try:
        # 1. Leer datos del cliente
        await cache.set_pipeline_status(cliente_id, "procesando", paso=0, detalle="Cargando datos del cliente")
        logger.info(f"Pipeline iniciado para cliente={cliente_id}")

        cliente = await db.obtener_cliente(cliente_id)
        nombre_cliente = cliente.get("nombre_cliente", "desconocido")
        datos_form = await db.obtener_datos_form(cliente_id)
        analisis_transcript_row = await db.obtener_analisis_transcript(cliente_id)

        if not analisis_transcript_row or not analisis_transcript_row.get("transcript_raw"):
            raise ValueError(f"No se encontró transcript_raw para cliente {cliente_id}")

        transcript_raw = analisis_transcript_row["transcript_raw"]

        # 2. Actualizar estado
        await db.actualizar_estado_pipeline(cliente_id, "procesando")
        await cache.set_pipeline_status(cliente_id, "procesando", paso=1, detalle="Ejecutando Paralelo A")

        # 3. Paralelo A: procesar_transcript + analizar_competidores
        # competidores es [{"nombre": "X"}, ...] — extraer solo los nombres
        competidores_lista = []
        if datos_form:
            comp = datos_form.get("competidores", [])
            if isinstance(comp, str):
                try:
                    comp = json.loads(comp)
                except json.JSONDecodeError:
                    comp = []
            if isinstance(comp, list):
                for c in comp:
                    if isinstance(c, dict):
                        nombre = c.get("nombre", "")
                        if nombre:
                            competidores_lista.append(nombre)
                    elif isinstance(c, str):
                        competidores_lista.append(c)

        transcript_task = procesar_transcript(cliente_id, transcript_raw)
        competitors_task = analizar_competidores(cliente_id, competidores_lista, cache)

        analisis_result, competidores_result = await asyncio.gather(
            transcript_task,
            competitors_task,
            return_exceptions=True,
        )

        # Verificar resultado del transcript (CRÍTICO)
        if isinstance(analisis_result, Exception):
            raise analisis_result

        # Guardar análisis de transcript en DB
        await db.guardar_analisis_transcript(cliente_id, analisis_result)
        await cache.set_pipeline_status(cliente_id, "procesando", paso=2, detalle="Transcript procesado")

        # Guardar competidores (no crítico)
        if isinstance(competidores_result, Exception):
            logger.warning(f"Análisis de competidores falló: {competidores_result}")
            competidores_result = []
        else:
            for comp_data in competidores_result:
                try:
                    await db.guardar_analisis_competidor(
                        cliente_id,
                        comp_data["competidor_nombre"],
                        comp_data,
                    )
                except Exception as e:
                    logger.warning(f"Error guardando competidor '{comp_data.get('competidor_nombre')}': {e}")

        # 4. Extraer keywords del transcript para Paralelo B
        keywords_transcript = analisis_result.get("keywords_mencionadas", [])
        keywords_base = []
        if datos_form:
            productos = datos_form.get("productos_top3", [])
            if isinstance(productos, str):
                try:
                    productos = json.loads(productos)
                except json.JSONDecodeError:
                    productos = []
            for p in productos:
                if isinstance(p, str):
                    keywords_base.append(p)
                elif isinstance(p, dict):
                    nombre = p.get("nombre", p.get("name", ""))
                    if nombre:
                        keywords_base.append(nombre)

        # 5. Paralelo B: keyword_search + keyword_volumes (ambos dentro de researchar_keywords)
        await cache.set_pipeline_status(cliente_id, "procesando", paso=3, detalle="Ejecutando keyword research")

        try:
            await researchar_keywords(
                cliente_id, keywords_base, keywords_transcript,
                datos_form or {}, db, cache,
            )
        except Exception as e:
            logger.warning(f"Keyword research falló (no crítico): {e}")

        # 6. Consolidar (placeholder)
        await cache.set_pipeline_status(cliente_id, "procesando", paso=5, detalle="Consolidando datos")
        contexto = await consolidar(cliente_id)

        # 7. Generar metodología (placeholder)
        await cache.set_pipeline_status(cliente_id, "procesando", paso=6, detalle="Generando metodología")
        metodologia = await generar_metodologia(cliente_id, contexto)

        # 8. Crear documento (placeholder)
        await cache.set_pipeline_status(cliente_id, "procesando", paso=7, detalle="Creando documento")
        documento_url = await crear_documento(cliente_id, metodologia)

        # 9. Completado
        await db.actualizar_estado_pipeline(cliente_id, "completado")
        await cache.set_pipeline_status(cliente_id, "completado", paso=8, detalle="Pipeline finalizado")

        # 10. Notificar via Telegram
        await enviar_telegram(
            f"✅ Pipeline completado para <b>{nombre_cliente}</b>\n"
            f"Cliente ID: <code>{cliente_id}</code>"
        )

        logger.info(f"Pipeline completado exitosamente para cliente={cliente_id}")

    except Exception as e:
        # Error crítico — garantizar notificación y actualización de estado
        error_msg = str(e)
        stack = traceback.format_exc()
        logger.error(f"Pipeline FALLÓ para cliente={cliente_id}: {error_msg}\n{stack}")

        try:
            await db.actualizar_estado_pipeline(cliente_id, "error")
        except Exception:
            logger.error("No se pudo actualizar estado a 'error' en DB")

        try:
            await cache.set_pipeline_status(
                cliente_id, "error", error=error_msg,
                detalle=f"Error en pipeline: {error_msg[:200]}",
            )
        except Exception:
            logger.error("No se pudo actualizar estado de error en Redis")

        try:
            await db.registrar_error(
                cliente_id, "pipeline", error_msg, stack_trace=stack,
            )
        except Exception:
            logger.error("No se pudo registrar error en log_errores")

        try:
            await enviar_telegram(
                f"❌ Pipeline FALLÓ para <b>{nombre_cliente}</b>\n"
                f"Error: <code>{error_msg[:500]}</code>"
            )
        except Exception:
            logger.error("No se pudo enviar notificación de error a Telegram")
