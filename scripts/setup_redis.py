"""
setup_redis.py — Verificación de Redis para el pipeline de marketing.

Uso:
    python setup_redis.py verify     # Verifica conexión y hace prueba SET/GET
    python setup_redis.py info       # Muestra información del servidor
    python setup_redis.py flush      # PELIGRO: limpia todas las keys

Requiere la variable de entorno REDIS_URL con la URL de conexión de Render.
"""

import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

import redis


def get_client():
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        print("ERROR: Variable de entorno REDIS_URL no configurada.")
        print("  Para servicios dentro de Render, usa la Internal Key Value URL:")
        print("  redis://red-xxx:password@red-xxx:6379")
        sys.exit(1)

    try:
        client = redis.from_url(redis_url, decode_responses=True, socket_connect_timeout=5)
        return client
    except Exception as e:
        print(f"ERROR de conexion: {e}")
        sys.exit(1)


def verify(client):
    print("Verificando conexion a Redis...\n")

    # 1. Ping
    try:
        result = client.ping()
        print(f"  PING: {'PONG - OK' if result else 'FALLO'}")
    except Exception as e:
        print(f"  PING fallo: {e}")
        return

    # 2. Prueba SET/GET
    print("\nPrueba SET/GET...")
    test_key = "__test_verificacion__"
    test_value = json.dumps({"status": "ok", "source": "setup_redis.py"})

    try:
        client.setex(test_key, 60, test_value)
        print(f"  SET '{test_key}' exitoso")

        retrieved = client.get(test_key)
        parsed = json.loads(retrieved)
        print(f"  GET '{test_key}': {parsed}")

        client.delete(test_key)
        print(f"  DELETE '{test_key}' exitoso")
    except Exception as e:
        print(f"  Error en prueba: {e}")
        return

    # 3. Verificar patrones de keys del pipeline
    print("\nPrueba de patrones de keys del pipeline...")

    patterns = {
        "meta_ads:test_competidor": {"competidor": "test", "datos": {}},
        "serp:test_keyword": {"keyword": "test", "datos": {}},
        "kwp:test_keyword": {"keyword": "test", "datos": {}},
        "pipeline:test_client:status": {"estado": "test", "paso": 0},
    }

    for key, value in patterns.items():
        try:
            client.setex(key, 60, json.dumps(value))
            retrieved = json.loads(client.get(key))
            client.delete(key)
            print(f"  Patron '{key.split(':')[0]}:*' funciona correctamente")
        except Exception as e:
            print(f"  Patron '{key}' fallo: {e}")

    print("\nRedis verificado y listo para el pipeline.")


def info(client):
    print("Informacion de Redis:\n")
    try:
        server_info = client.info()
        print(f"  Version:            {server_info.get('redis_version', 'unknown')}")
        print(f"  Memoria usada:      {server_info.get('used_memory_human', 'unknown')}")
        print(f"  Memoria maxima:     {server_info.get('maxmemory_human', 'unknown')}")
        print(f"  Clientes conectados: {server_info.get('connected_clients', 0)}")
        print(f"  Keys totales:       {client.dbsize()}")
        print(f"  Uptime (dias):      {server_info.get('uptime_in_days', 0)}")

        total = client.dbsize()
        if 0 < total <= 50:
            print(f"\n  Keys existentes:")
            for key in client.keys("*"):
                ttl = client.ttl(key)
                print(f"    - {key} (TTL: {ttl}s)")
    except Exception as e:
        print(f"  Error: {e}")


def flush(client):
    print("FLUSH: Eliminando TODAS las keys de Redis...")
    confirm = input("  Estas seguro? Escribe 'SI' para confirmar: ")
    if confirm != "SI":
        print("  Cancelado.")
        return

    try:
        client.flushdb()
        print("  Todas las keys eliminadas.")
    except Exception as e:
        print(f"  Error: {e}")


def main():
    if len(sys.argv) < 2:
        print("Uso: python setup_redis.py [verify|info|flush]")
        sys.exit(1)

    command = sys.argv[1].lower()
    client = get_client()

    if command == "verify":
        verify(client)
    elif command == "info":
        info(client)
    elif command == "flush":
        flush(client)
    else:
        print(f"Comando desconocido: {command}")
        print("Opciones: verify, info, flush")


if __name__ == "__main__":
    main()
