"""
ETAPA 0.1 — Configuración de PostgreSQL
Script de migración y verificación de la base de datos.

Uso:
    python setup_database.py migrate    # Crea todas las tablas
    python setup_database.py verify     # Verifica la conexión y estructura
    python setup_database.py reset      # PELIGRO: elimina todo y recrea (solo desarrollo)

Requiere la variable de entorno DATABASE_URL con la cadena de conexión de Render.
Ejemplo: postgresql://user:password@host:port/dbname
"""

import os
import sys
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Conexión
# ---------------------------------------------------------------------------

def get_connection():
    """Obtiene conexión a PostgreSQL usando DATABASE_URL de Render."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("❌ ERROR: Variable de entorno DATABASE_URL no configurada.")
        print("   Obtén la URL de conexión desde el dashboard de Render:")
        print("   Render > PostgreSQL > Info > External Database URL")
        sys.exit(1)

    try:
        conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ ERROR de conexión: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Migración
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
-- Extensión para UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Tipos ENUM
DO $$ BEGIN
    CREATE TYPE estado_pipeline_enum AS ENUM (
        'carpeta_creada', 'esperando_transcript', 'transcript_listo', 'procesando', 'completado', 'error'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE estado_revision_enum AS ENUM (
        'pendiente', 'aprobada', 'requiere_cambios'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE ia_provider_enum AS ENUM (
        'openai', 'anthropic', 'gemini'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Función para actualizar fecha_actualizacion
CREATE OR REPLACE FUNCTION actualizar_fecha_actualizacion()
RETURNS TRIGGER AS $$
BEGIN
    NEW.fecha_actualizacion = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 1. clientes
CREATE TABLE IF NOT EXISTS clientes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nombre_cliente TEXT NOT NULL UNIQUE,
    email_cliente TEXT,
    carpeta_drive_id TEXT,
    estado_pipeline estado_pipeline_enum NOT NULL DEFAULT 'carpeta_creada',
    fecha_creacion TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    fecha_actualizacion TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clientes_estado ON clientes (estado_pipeline);

DROP TRIGGER IF EXISTS trg_clientes_actualizacion ON clientes;
CREATE TRIGGER trg_clientes_actualizacion
    BEFORE UPDATE ON clientes
    FOR EACH ROW
    EXECUTE FUNCTION actualizar_fecha_actualizacion();

-- 2. datos_form
CREATE TABLE IF NOT EXISTS datos_form (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cliente_id UUID NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
    proposito_negocio TEXT,
    diferenciadores TEXT,
    descripcion_deseada TEXT,
    productos_top3 JSONB DEFAULT '[]'::jsonb,
    producto_exclusivo TEXT,
    precios_promedio TEXT,
    ofertas_especiales TEXT,
    publico_objetivo TEXT,
    problemas_que_resuelve TEXT,
    competidores JSONB DEFAULT '[]'::jsonb,
    errores_competencia TEXT,
    productos_a_promocionar JSONB DEFAULT '[]'::jsonb,
    fecha_recepcion TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_datos_form_cliente ON datos_form (cliente_id);

-- 3. analisis_transcript
CREATE TABLE IF NOT EXISTS analisis_transcript (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cliente_id UUID NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
    transcript_raw TEXT,
    resumen_ejecutivo TEXT,
    objetivos_cliente JSONB DEFAULT '[]'::jsonb,
    keywords_mencionadas JSONB DEFAULT '[]'::jsonb,
    metodologias_discutidas JSONB DEFAULT '[]'::jsonb,
    acuerdos_decisiones JSONB DEFAULT '[]'::jsonb,
    tono_personalidad TEXT,
    fecha_procesamiento TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_analisis_transcript_cliente ON analisis_transcript (cliente_id);

-- 4. analisis_competidores
CREATE TABLE IF NOT EXISTS analisis_competidores (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cliente_id UUID NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
    competidor_nombre TEXT NOT NULL,
    creativos_activos JSONB DEFAULT '[]'::jsonb,
    copies JSONB DEFAULT '[]'::jsonb,
    formatos_anuncio JSONB DEFAULT '[]'::jsonb,
    tiempo_activo JSONB DEFAULT '{}'::jsonb,
    plataformas JSONB DEFAULT '[]'::jsonb,
    cantidad_anuncios INTEGER DEFAULT 0,
    competidor_page_url TEXT,
    anuncios_raw JSONB DEFAULT '[]'::jsonb,
    creativos_urls JSONB DEFAULT '[]'::jsonb,
    analisis_por_anuncio JSONB DEFAULT '[]'::jsonb,
    patrones_messaging TEXT,
    oportunidades_detectadas TEXT,
    sintesis_competitiva TEXT,
    fecha_consulta TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_competidor_por_cliente 
    ON analisis_competidores (cliente_id, competidor_nombre);
CREATE INDEX IF NOT EXISTS idx_analisis_competidores_cliente 
    ON analisis_competidores (cliente_id);

-- 5. keyword_research
CREATE TABLE IF NOT EXISTS keyword_research (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cliente_id UUID NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
    keyword TEXT NOT NULL,
    resultados_organicos JSONB DEFAULT '[]'::jsonb,
    anuncios_encontrados JSONB DEFAULT '[]'::jsonb,
    people_also_ask JSONB DEFAULT '[]'::jsonb,
    busquedas_relacionadas JSONB DEFAULT '[]'::jsonb,
    sugerencias_autocomplete JSONB DEFAULT '[]'::jsonb,
    tipo_contenido_posiciona TEXT,
    volumen_busqueda INTEGER,
    nivel_competencia TEXT,
    cpc_estimado DECIMAL(10, 2),
    tendencia JSONB DEFAULT '[]'::jsonb,
    fecha_consulta TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_keyword_por_cliente 
    ON keyword_research (cliente_id, keyword);
CREATE INDEX IF NOT EXISTS idx_keyword_research_cliente 
    ON keyword_research (cliente_id);

-- 6. metodologias_generadas
CREATE TABLE IF NOT EXISTS metodologias_generadas (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cliente_id UUID NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
    documento_drive_id TEXT,
    ia_utilizada ia_provider_enum,
    prompt_utilizado TEXT,
    contenido_generado TEXT,
    intentos_generacion INTEGER DEFAULT 1,
    secciones_completadas JSONB DEFAULT '[]'::jsonb,
    fecha_generacion TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    estado_revision estado_revision_enum DEFAULT 'pendiente'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_metodologia_cliente ON metodologias_generadas (cliente_id);

-- 7. log_errores
CREATE TABLE IF NOT EXISTS log_errores (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cliente_id UUID REFERENCES clientes(id) ON DELETE SET NULL,
    etapa TEXT NOT NULL,
    mensaje_error TEXT NOT NULL,
    stack_trace TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    fecha TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_log_errores_cliente ON log_errores (cliente_id);
CREATE INDEX IF NOT EXISTS idx_log_errores_fecha ON log_errores (fecha DESC);
"""


def migrate(conn):
    """Ejecuta la migración completa."""
    print("🔄 Ejecutando migración...")
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
        print("✅ Migración completada exitosamente.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error en migración: {e}")
        raise


# ---------------------------------------------------------------------------
# Verificación
# ---------------------------------------------------------------------------

EXPECTED_TABLES = [
    "clientes",
    "datos_form",
    "analisis_transcript",
    "analisis_competidores",
    "keyword_research",
    "metodologias_generadas",
    "log_errores",
]


def verify(conn):
    """Verifica que todas las tablas existen y hace una prueba de inserción/lectura."""
    print("🔍 Verificando esquema de base de datos...\n")

    with conn.cursor() as cur:
        # 1. Verificar tablas
        cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """)
        existing_tables = [row["table_name"] for row in cur.fetchall()]

        all_ok = True
        for table in EXPECTED_TABLES:
            if table in existing_tables:
                # Contar columnas
                cur.execute("""
                    SELECT COUNT(*) as col_count 
                    FROM information_schema.columns 
                    WHERE table_name = %s AND table_schema = 'public';
                """, (table,))
                col_count = cur.fetchone()["col_count"]
                print(f"   ✅ {table} ({col_count} columnas)")
            else:
                print(f"   ❌ {table} — NO ENCONTRADA")
                all_ok = False

        if not all_ok:
            print("\n❌ Faltan tablas. Ejecuta: python setup_database.py migrate")
            return

        # 2. Prueba de inserción y lectura
        print("\n🧪 Prueba de inserción/lectura...")
        try:
            cur.execute("""
                INSERT INTO clientes (nombre_cliente, email_cliente, estado_pipeline)
                VALUES ('__test_verificacion__', 'test@test.com', 'esperando_transcript')
                RETURNING id, nombre_cliente, estado_pipeline, fecha_creacion;
            """)
            result = cur.fetchone()
            print(f"   ✅ INSERT exitoso — id: {result['id']}")
            print(f"   ✅ Estado: {result['estado_pipeline']}")
            print(f"   ✅ Fecha: {result['fecha_creacion']}")

            # Limpiar
            cur.execute("DELETE FROM clientes WHERE nombre_cliente = '__test_verificacion__';")
            conn.commit()
            print("   ✅ Limpieza de prueba completada")

        except Exception as e:
            conn.rollback()
            print(f"   ❌ Error en prueba: {e}")
            return

        # 3. Verificar enums
        print("\n🔍 Verificando tipos ENUM...")
        cur.execute("""
            SELECT typname, array_agg(enumlabel ORDER BY enumsortorder) as values
            FROM pg_enum e
            JOIN pg_type t ON e.enumtypid = t.oid
            GROUP BY typname;
        """)
        for row in cur.fetchall():
            print(f"   ✅ {row['typname']}: {row['values']}")

        # 4. Verificar trigger
        print("\n🔍 Verificando triggers...")
        cur.execute("""
            SELECT trigger_name, event_manipulation, action_timing
            FROM information_schema.triggers
            WHERE trigger_schema = 'public';
        """)
        triggers = cur.fetchall()
        if triggers:
            for t in triggers:
                print(f"   ✅ {t['trigger_name']} ({t['action_timing']} {t['event_manipulation']})")
        else:
            print("   ⚠️  No se encontraron triggers")

    print("\n✅ Verificación completa. Base de datos lista.")


# ---------------------------------------------------------------------------
# Reset (solo desarrollo)
# ---------------------------------------------------------------------------

def reset(conn):
    """Elimina todo y recrea. SOLO PARA DESARROLLO."""
    print("⚠️  RESET: Eliminando todas las tablas y tipos...")
    confirm = input("   ¿Estás seguro? Escribe 'SI' para confirmar: ")
    if confirm != "SI":
        print("   Cancelado.")
        return

    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS log_errores CASCADE;
            DROP TABLE IF EXISTS metodologias_generadas CASCADE;
            DROP TABLE IF EXISTS keyword_research CASCADE;
            DROP TABLE IF EXISTS analisis_competidores CASCADE;
            DROP TABLE IF EXISTS analisis_transcript CASCADE;
            DROP TABLE IF EXISTS datos_form CASCADE;
            DROP TABLE IF EXISTS clientes CASCADE;
            DROP TYPE IF EXISTS estado_pipeline_enum CASCADE;
            DROP TYPE IF EXISTS estado_revision_enum CASCADE;
            DROP TYPE IF EXISTS ia_provider_enum CASCADE;
            DROP FUNCTION IF EXISTS actualizar_fecha_actualizacion CASCADE;
        """)
    conn.commit()
    print("   ✅ Todo eliminado.")

    migrate(conn)
    verify(conn)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Uso: python setup_database.py [migrate|verify|reset]")
        sys.exit(1)

    command = sys.argv[1].lower()
    conn = get_connection()

    try:
        if command == "migrate":
            migrate(conn)
        elif command == "verify":
            verify(conn)
        elif command == "reset":
            reset(conn)
        else:
            print(f"Comando desconocido: {command}")
            print("Opciones: migrate, verify, reset")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
