"""
db/client.py — Capa de acceso a datos async para el pipeline de marketing.

Encapsula las operaciones CRUD de cada tabla usando asyncpg.
Se usa en todos los endpoints de FastAPI (Etapas 1-6).

Uso:
    from db.client import DatabaseClient

    db = DatabaseClient(database_url)
    await db.connect()

    cliente = await db.crear_cliente("Mi Negocio", "correo@ejemplo.com", "drive_folder_123")
    await db.actualizar_estado_pipeline(cliente["id"], "procesando")

    await db.close()
"""

import os
import json
import logging
from typing import Optional
from datetime import datetime, timezone

import asyncpg

logger = logging.getLogger(__name__)


class DatabaseClient:
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.environ.get("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL no configurada")
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Crea el pool de conexiones."""
        self._pool = await asyncpg.create_pool(
            self.database_url,
            min_size=2,
            max_size=10,
            command_timeout=60,
        )
        logger.info("Pool de conexiones PostgreSQL creado")

    async def close(self):
        """Cierra el pool de conexiones."""
        if self._pool:
            await self._pool.close()
            logger.info("Pool de conexiones PostgreSQL cerrado")

    def _row_to_dict(self, row: asyncpg.Record) -> dict:
        """Convierte un asyncpg.Record a dict con serialización JSON correcta."""
        if row is None:
            return None
        d = dict(row)
        # Convertir valores que asyncpg devuelve como strings JSON a dicts/lists
        for k, v in d.items():
            if isinstance(v, str):
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, (dict, list)):
                        d[k] = parsed
                except (json.JSONDecodeError, TypeError):
                    pass
        return d

    # -----------------------------------------------------------------
    # CLIENTES
    # -----------------------------------------------------------------

    async def crear_cliente(self, nombre: str, email: str = None, carpeta_drive_id: str = None) -> dict:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO clientes (nombre_cliente, email_cliente, carpeta_drive_id, estado_pipeline)
                VALUES ($1, $2, $3, 'esperando_transcript')
                RETURNING *;
            """, nombre, email, carpeta_drive_id)
            return self._row_to_dict(row)

    async def obtener_cliente(self, cliente_id: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM clientes WHERE id = $1;", cliente_id)
            return self._row_to_dict(row) if row else None

    async def obtener_cliente_por_nombre(self, nombre: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM clientes WHERE nombre_cliente = $1;", nombre)
            return self._row_to_dict(row) if row else None

    async def actualizar_estado_pipeline(self, cliente_id: str, estado: str) -> dict:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                UPDATE clientes SET estado_pipeline = $1
                WHERE id = $2 RETURNING *;
            """, estado, cliente_id)
            return self._row_to_dict(row)

    async def actualizar_carpeta_drive(self, cliente_id: str, carpeta_drive_id: str) -> dict:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                UPDATE clientes SET carpeta_drive_id = $1
                WHERE id = $2 RETURNING *;
            """, carpeta_drive_id, cliente_id)
            return self._row_to_dict(row)

    # -----------------------------------------------------------------
    # DATOS DEL FORM
    # -----------------------------------------------------------------

    async def guardar_datos_form(self, cliente_id: str, datos: dict) -> dict:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO datos_form (
                    cliente_id, proposito_negocio, diferenciadores,
                    descripcion_deseada, productos_top3, producto_exclusivo,
                    precios_promedio, ofertas_especiales, publico_objetivo,
                    problemas_que_resuelve, competidores, errores_competencia,
                    productos_a_promocionar
                ) VALUES (
                    $1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11::jsonb, $12, $13::jsonb
                )
                ON CONFLICT (cliente_id) DO UPDATE SET
                    proposito_negocio = EXCLUDED.proposito_negocio,
                    diferenciadores = EXCLUDED.diferenciadores,
                    descripcion_deseada = EXCLUDED.descripcion_deseada,
                    productos_top3 = EXCLUDED.productos_top3,
                    producto_exclusivo = EXCLUDED.producto_exclusivo,
                    precios_promedio = EXCLUDED.precios_promedio,
                    ofertas_especiales = EXCLUDED.ofertas_especiales,
                    publico_objetivo = EXCLUDED.publico_objetivo,
                    problemas_que_resuelve = EXCLUDED.problemas_que_resuelve,
                    competidores = EXCLUDED.competidores,
                    errores_competencia = EXCLUDED.errores_competencia,
                    productos_a_promocionar = EXCLUDED.productos_a_promocionar,
                    fecha_recepcion = NOW()
                RETURNING *;
            """,
                cliente_id,
                datos.get("proposito_negocio"),
                datos.get("diferenciadores"),
                datos.get("descripcion_deseada"),
                json.dumps(datos.get("productos_top3", []), ensure_ascii=False),
                datos.get("producto_exclusivo"),
                datos.get("precios_promedio"),
                datos.get("ofertas_especiales"),
                datos.get("publico_objetivo"),
                datos.get("problemas_que_resuelve"),
                json.dumps(datos.get("competidores", []), ensure_ascii=False),
                datos.get("errores_competencia"),
                json.dumps(datos.get("productos_a_promocionar", []), ensure_ascii=False),
            )
            return self._row_to_dict(row)

    async def obtener_datos_form(self, cliente_id: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM datos_form WHERE cliente_id = $1;", cliente_id)
            return self._row_to_dict(row) if row else None

    # -----------------------------------------------------------------
    # ANÁLISIS TRANSCRIPT
    # -----------------------------------------------------------------

    async def guardar_transcript_raw(self, cliente_id: str, transcript_raw: str) -> dict:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO analisis_transcript (cliente_id, transcript_raw)
                VALUES ($1, $2)
                ON CONFLICT (cliente_id) DO UPDATE SET
                    transcript_raw = EXCLUDED.transcript_raw,
                    fecha_procesamiento = NOW()
                RETURNING *;
            """, cliente_id, transcript_raw)
            return self._row_to_dict(row)

    async def guardar_analisis_transcript(self, cliente_id: str, analisis: dict) -> dict:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                UPDATE analisis_transcript SET
                    resumen_ejecutivo = $1,
                    objetivos_cliente = $2::jsonb,
                    keywords_mencionadas = $3::jsonb,
                    metodologias_discutidas = $4::jsonb,
                    acuerdos_decisiones = $5::jsonb,
                    tono_personalidad = $6,
                    fecha_procesamiento = NOW()
                WHERE cliente_id = $7
                RETURNING *;
            """,
                analisis.get("resumen_ejecutivo"),
                json.dumps(analisis.get("objetivos_cliente", []), ensure_ascii=False),
                json.dumps(analisis.get("keywords_mencionadas", []), ensure_ascii=False),
                json.dumps(analisis.get("metodologias_discutidas", []), ensure_ascii=False),
                json.dumps(analisis.get("acuerdos_decisiones", []), ensure_ascii=False),
                analisis.get("tono_personalidad"),
                cliente_id,
            )
            return self._row_to_dict(row)

    async def obtener_analisis_transcript(self, cliente_id: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM analisis_transcript WHERE cliente_id = $1;", cliente_id)
            return self._row_to_dict(row) if row else None

    # -----------------------------------------------------------------
    # ANÁLISIS COMPETIDORES
    # -----------------------------------------------------------------

    async def guardar_analisis_competidor(self, cliente_id: str, competidor: str, datos: dict) -> dict:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO analisis_competidores (
                    cliente_id, competidor_nombre, competidor_page_url,
                    creativos_activos, copies, formatos_anuncio,
                    tiempo_activo, plataformas, cantidad_anuncios,
                    anuncios_raw, creativos_urls, analisis_por_anuncio,
                    patrones_messaging, oportunidades_detectadas, sintesis_competitiva
                ) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9,
                          $10::jsonb, $11::jsonb, $12::jsonb, $13, $14, $15)
                ON CONFLICT (cliente_id, competidor_nombre) DO UPDATE SET
                    competidor_page_url = EXCLUDED.competidor_page_url,
                    creativos_activos = EXCLUDED.creativos_activos,
                    copies = EXCLUDED.copies,
                    formatos_anuncio = EXCLUDED.formatos_anuncio,
                    tiempo_activo = EXCLUDED.tiempo_activo,
                    plataformas = EXCLUDED.plataformas,
                    cantidad_anuncios = EXCLUDED.cantidad_anuncios,
                    anuncios_raw = EXCLUDED.anuncios_raw,
                    creativos_urls = EXCLUDED.creativos_urls,
                    analisis_por_anuncio = EXCLUDED.analisis_por_anuncio,
                    patrones_messaging = EXCLUDED.patrones_messaging,
                    oportunidades_detectadas = EXCLUDED.oportunidades_detectadas,
                    sintesis_competitiva = EXCLUDED.sintesis_competitiva,
                    fecha_consulta = NOW()
                RETURNING *;
            """,
                cliente_id,
                competidor,
                datos.get("competidor_page_url"),
                json.dumps(datos.get("creativos_activos", []), ensure_ascii=False),
                json.dumps(datos.get("copies", []), ensure_ascii=False),
                json.dumps(datos.get("formatos_anuncio", []), ensure_ascii=False),
                json.dumps(datos.get("tiempo_activo", {}), ensure_ascii=False),
                json.dumps(datos.get("plataformas", []), ensure_ascii=False),
                datos.get("cantidad_anuncios", 0),
                json.dumps(datos.get("anuncios_raw", []), ensure_ascii=False),
                json.dumps(datos.get("creativos_urls", []), ensure_ascii=False),
                json.dumps(datos.get("analisis_por_anuncio", []), ensure_ascii=False),
                datos.get("patrones_messaging"),
                datos.get("oportunidades_detectadas"),
                datos.get("sintesis_competitiva"),
            )
            return self._row_to_dict(row)

    async def obtener_competidores(self, cliente_id: str) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM analisis_competidores WHERE cliente_id = $1;",
                cliente_id,
            )
            return [self._row_to_dict(row) for row in rows]

    # -----------------------------------------------------------------
    # KEYWORD RESEARCH
    # -----------------------------------------------------------------

    async def guardar_keyword_serp(self, cliente_id: str, keyword: str, datos: dict) -> dict:
        """Guarda resultados de SerpAPI para una keyword."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO keyword_research (
                    cliente_id, keyword, resultados_organicos, anuncios_encontrados,
                    people_also_ask, busquedas_relacionadas, sugerencias_autocomplete,
                    tipo_contenido_posiciona
                ) VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb, $8)
                ON CONFLICT (cliente_id, keyword) DO UPDATE SET
                    resultados_organicos = EXCLUDED.resultados_organicos,
                    anuncios_encontrados = EXCLUDED.anuncios_encontrados,
                    people_also_ask = EXCLUDED.people_also_ask,
                    busquedas_relacionadas = EXCLUDED.busquedas_relacionadas,
                    sugerencias_autocomplete = EXCLUDED.sugerencias_autocomplete,
                    tipo_contenido_posiciona = EXCLUDED.tipo_contenido_posiciona,
                    fecha_consulta = NOW()
                RETURNING *;
            """,
                cliente_id,
                keyword,
                json.dumps(datos.get("resultados_organicos", []), ensure_ascii=False),
                json.dumps(datos.get("anuncios_encontrados", []), ensure_ascii=False),
                json.dumps(datos.get("people_also_ask", []), ensure_ascii=False),
                json.dumps(datos.get("busquedas_relacionadas", []), ensure_ascii=False),
                json.dumps(datos.get("sugerencias_autocomplete", []), ensure_ascii=False),
                datos.get("tipo_contenido_posiciona"),
            )
            return self._row_to_dict(row)

    async def actualizar_keyword_volumenes(self, cliente_id: str, keyword: str, datos: dict) -> dict:
        """Actualiza con datos de Google Keyword Planner."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                UPDATE keyword_research SET
                    volumen_busqueda = $1,
                    nivel_competencia = $2,
                    cpc_estimado = $3,
                    tendencia = $4::jsonb
                WHERE cliente_id = $5 AND keyword = $6
                RETURNING *;
            """,
                datos.get("volumen_busqueda"),
                datos.get("nivel_competencia"),
                datos.get("cpc_estimado"),
                json.dumps(datos.get("tendencia", []), ensure_ascii=False),
                cliente_id,
                keyword,
            )
            return self._row_to_dict(row) if row else None

    async def obtener_keywords(self, cliente_id: str) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM keyword_research WHERE cliente_id = $1;",
                cliente_id,
            )
            return [self._row_to_dict(row) for row in rows]

    # -----------------------------------------------------------------
    # METODOLOGÍAS GENERADAS
    # -----------------------------------------------------------------

    async def guardar_metodologia(self, cliente_id: str, datos: dict) -> dict:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO metodologias_generadas (
                    cliente_id, documento_drive_id, ia_utilizada,
                    prompt_utilizado, contenido_generado, intentos_generacion,
                    secciones_completadas, estado_revision
                ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
                ON CONFLICT (cliente_id) DO UPDATE SET
                    documento_drive_id = EXCLUDED.documento_drive_id,
                    ia_utilizada = EXCLUDED.ia_utilizada,
                    prompt_utilizado = EXCLUDED.prompt_utilizado,
                    contenido_generado = EXCLUDED.contenido_generado,
                    intentos_generacion = EXCLUDED.intentos_generacion,
                    secciones_completadas = EXCLUDED.secciones_completadas,
                    estado_revision = EXCLUDED.estado_revision,
                    fecha_generacion = NOW()
                RETURNING *;
            """,
                cliente_id,
                datos.get("documento_drive_id"),
                datos.get("ia_utilizada"),
                datos.get("prompt_utilizado"),
                datos.get("contenido_generado"),
                datos.get("intentos_generacion", 1),
                json.dumps(datos.get("secciones_completadas", []), ensure_ascii=False),
                datos.get("estado_revision", "pendiente"),
            )
            return self._row_to_dict(row)

    async def obtener_metodologia(self, cliente_id: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM metodologias_generadas WHERE cliente_id = $1;",
                cliente_id,
            )
            return self._row_to_dict(row) if row else None

    # -----------------------------------------------------------------
    # LOG DE ERRORES
    # -----------------------------------------------------------------

    async def registrar_error(self, cliente_id: str, etapa: str, mensaje: str,
                              stack_trace: str = None, metadata: dict = None) -> dict:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO log_errores (cliente_id, etapa, mensaje_error, stack_trace, metadata)
                VALUES ($1, $2, $3, $4, $5::jsonb)
                RETURNING *;
            """,
                cliente_id,
                etapa,
                mensaje,
                stack_trace,
                json.dumps(metadata or {}, ensure_ascii=False),
            )
            return self._row_to_dict(row)

    async def obtener_errores(self, cliente_id: str = None, limit: int = 50) -> list[dict]:
        async with self._pool.acquire() as conn:
            if cliente_id:
                rows = await conn.fetch(
                    "SELECT * FROM log_errores WHERE cliente_id = $1 ORDER BY fecha DESC LIMIT $2;",
                    cliente_id, limit,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM log_errores ORDER BY fecha DESC LIMIT $1;",
                    limit,
                )
            return [self._row_to_dict(row) for row in rows]

    # -----------------------------------------------------------------
    # CONTEXTO COMPLETO (para Etapa 4 — Consolidación)
    # -----------------------------------------------------------------

    async def obtener_contexto_completo(self, cliente_id: str) -> dict:
        """Obtiene toda la información del cliente para construir el prompt."""
        cliente = await self.obtener_cliente(cliente_id)
        if not cliente:
            raise ValueError(f"Cliente {cliente_id} no encontrado")

        return {
            "cliente": cliente,
            "datos_form": await self.obtener_datos_form(cliente_id),
            "analisis_transcript": await self.obtener_analisis_transcript(cliente_id),
            "competidores": await self.obtener_competidores(cliente_id),
            "keywords": await self.obtener_keywords(cliente_id),
            "metodologia": await self.obtener_metodologia(cliente_id),
        }
