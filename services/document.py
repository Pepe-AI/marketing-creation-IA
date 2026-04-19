"""
services/document.py — Etapa 6: Creación de Google Doc + notificación Telegram.

Recibe la metodología generada por Etapa 5, crea un Google Doc con formato
profesional en la carpeta Drive del cliente, actualiza el registro en DB,
y envía notificación de entrega por Telegram.
"""

import os
import re
import json
import logging
from datetime import datetime, timezone

from googleapiclient.discovery import build
from google.oauth2 import service_account

from utils.telegram import enviar_telegram

logger = logging.getLogger(__name__)

# Scopes necesarios para Drive y Docs
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]

# Títulos de las 9 secciones
SECTION_TITLES = {
    1: "1. Análisis de Situación",
    2: "2. Audiencia Objetivo",
    3: "3. Propuesta de Valor",
    4: "4. Estrategia de Canales",
    5: "5. Plan de Contenido",
    6: "6. Análisis Competitivo",
    7: "7. Estrategia de Keywords",
    8: "8. KPIs y Métricas",
    9: "9. Timeline Recomendado",
}


# =====================================================================
# Inicialización de clientes Google
# =====================================================================

_drive_service = None
_docs_service = None


def _get_google_services():
    """Inicializa y cachea los clientes de Drive y Docs."""
    global _drive_service, _docs_service

    if _drive_service and _docs_service:
        return _drive_service, _docs_service

    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON no configurada")

    creds_dict = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES,
    )

    _drive_service = build("drive", "v3", credentials=credentials)
    _docs_service = build("docs", "v1", credentials=credentials)

    return _drive_service, _docs_service


# =====================================================================
# Extracción de secciones del texto generado
# =====================================================================

def _limpiar_markdown(texto: str) -> str:
    """Elimina markdown residual que no se renderiza en Google Docs."""
    # Eliminar headers markdown (##, ###)
    texto = re.sub(r'^#{1,4}\s*', '', texto, flags=re.MULTILINE)
    # Eliminar negritas markdown (**texto** o __texto__)
    texto = re.sub(r'\*\*(.*?)\*\*', r'\1', texto)
    texto = re.sub(r'__(.*?)__', r'\1', texto)
    # Eliminar cursivas markdown (*texto* o _texto_ — cuidado con no romper contracciones)
    texto = re.sub(r'(?<!\w)\*([^*]+)\*(?!\w)', r'\1', texto)
    # Eliminar separadores ---
    texto = re.sub(r'^-{3,}$', '', texto, flags=re.MULTILINE)
    # Normalizar saltos de línea
    texto = texto.replace('\r\n', '\n')
    return texto.strip()


def extraer_secciones(texto: str) -> list:
    """
    Parsea el texto generado por la IA y extrae las 9 secciones.
    Busca marcadores tipo "SECCIÓN X:" o "## SECCIÓN X" o variantes.
    """
    if not texto:
        return []

    texto = _limpiar_markdown(texto)

    # Patrón flexible: "SECCIÓN N:", "## SECCIÓN N:", "N. Título", etc.
    patron = re.compile(
        r'(?:^|\n)\s*(?:##?\s*)?'                           # Opcional: ## o #
        r'(?:SECCI[OÓ]N\s+(\d+)\s*[:\-—]\s*(.+?))'         # SECCIÓN N: Título
        r'|'
        r'(?:^|\n)\s*(?:##?\s*)?'
        r'(\d+)\.\s+(.+)',                                   # N. Título
        re.IGNORECASE | re.MULTILINE,
    )

    matches = list(patron.finditer(texto))

    if not matches:
        # Fallback: tratar todo como una sola sección
        logger.warning("No se encontraron marcadores de sección en el texto")
        return [{"numero": 0, "titulo": "Metodología de Marketing", "contenido": texto}]

    secciones = []
    for i, match in enumerate(matches):
        # Extraer número y título del match
        if match.group(1):  # Formato "SECCIÓN N: Título"
            numero = int(match.group(1))
            titulo = match.group(2).strip()
        else:  # Formato "N. Título"
            numero = int(match.group(3))
            titulo = match.group(4).strip()

        # El contenido va desde el fin de este match hasta el inicio del siguiente
        inicio_contenido = match.end()
        fin_contenido = matches[i + 1].start() if i + 1 < len(matches) else len(texto)
        contenido = texto[inicio_contenido:fin_contenido].strip()

        # Usar título del mapa si existe, sino el encontrado
        titulo_final = SECTION_TITLES.get(numero, f"{numero}. {titulo}")

        secciones.append({
            "numero": numero,
            "titulo": titulo_final,
            "contenido": contenido,
        })

    logger.info(f"Secciones extraídas: {len(secciones)}")
    return secciones


# =====================================================================
# Creación del Google Doc
# =====================================================================

def _crear_google_doc(nombre_cliente: str, carpeta_drive_id: str, secciones: list) -> dict:
    """
    Crea un Google Doc en Drive y aplica formato.
    Retorna dict con document_id y web_view_link.
    """
    drive_service, docs_service = _get_google_services()
    fecha_hoy = datetime.now().strftime("%d de %B de %Y")

    # 1. Crear el archivo en Drive con la carpeta padre asignada
    file_metadata = {
        "name": f"Estrategia de Marketing — {nombre_cliente}",
        "mimeType": "application/vnd.google-apps.document",
        "parents": [carpeta_drive_id],
    }
    doc_file = drive_service.files().create(
        body=file_metadata,
        fields="id, webViewLink",
    ).execute()
    document_id = doc_file["id"]
    web_view_link = doc_file.get("webViewLink", f"https://docs.google.com/document/d/{document_id}/edit")

    logger.info(f"Google Doc creado: {document_id}")

    # 2. Construir el texto completo del documento
    bloques = []
    # Título del documento
    bloques.append({"texto": f"Estrategia de Marketing — {nombre_cliente}\n", "estilo": "TITLE"})
    # Subtítulo
    bloques.append({"texto": f"Preparado por Ecosfera Digital — {fecha_hoy}\n\n", "estilo": "SUBTITLE"})

    # Secciones
    for seccion in secciones:
        bloques.append({"texto": f"{seccion['titulo']}\n", "estilo": "HEADING_1"})
        # Limitar contenido por sección (50K chars máx por seguridad)
        contenido = seccion["contenido"][:50000]
        bloques.append({"texto": f"{contenido}\n\n", "estilo": "NORMAL_TEXT"})

    # 3. Primera llamada: insertar todo el texto de una vez
    texto_completo = "".join(b["texto"] for b in bloques)
    insert_requests = [
        {"insertText": {"location": {"index": 1}, "text": texto_completo}},
    ]
    docs_service.documents().batchUpdate(
        documentId=document_id,
        body={"requests": insert_requests},
    ).execute()

    # 4. Segunda llamada: leer el doc para obtener rangos reales y aplicar estilos
    doc = docs_service.documents().get(documentId=document_id).execute()
    body_content = doc.get("body", {}).get("content", [])

    style_requests = []
    for element in body_content:
        paragraph = element.get("paragraph")
        if not paragraph:
            continue

        # Extraer texto del párrafo
        paragraph_text = ""
        for elem in paragraph.get("elements", []):
            text_run = elem.get("textRun", {})
            paragraph_text += text_run.get("content", "")

        paragraph_text_clean = paragraph_text.strip()
        if not paragraph_text_clean:
            continue

        start_index = element.get("startIndex", 0)
        end_index = element.get("endIndex", 0)

        # Detectar qué estilo aplicar según el contenido del párrafo
        estilo = None
        if paragraph_text_clean == f"Estrategia de Marketing — {nombre_cliente}":
            estilo = "TITLE"
        elif paragraph_text_clean.startswith("Preparado por Ecosfera Digital"):
            estilo = "SUBTITLE"
        else:
            # Verificar si es un título de sección
            for titulo in SECTION_TITLES.values():
                if paragraph_text_clean == titulo:
                    estilo = "HEADING_1"
                    break

        if estilo:
            style_requests.append({
                "updateParagraphStyle": {
                    "range": {"startIndex": start_index, "endIndex": end_index},
                    "paragraphStyle": {"namedStyleType": estilo},
                    "fields": "namedStyleType",
                },
            })

    if style_requests:
        docs_service.documents().batchUpdate(
            documentId=document_id,
            body={"requests": style_requests},
        ).execute()
        logger.info(f"Estilos aplicados: {len(style_requests)} párrafos formateados")

    return {"document_id": document_id, "web_view_link": web_view_link}


# =====================================================================
# Cálculo de tiempo de procesamiento
# =====================================================================

async def _calcular_tiempo(cliente_id: str, cache) -> str:
    """Calcula tiempo total de procesamiento desde Redis o fallback."""
    try:
        if cache:
            status = await cache.get_pipeline_status(cliente_id)
            if status and "timestamp" in status:
                inicio = datetime.fromisoformat(status["timestamp"])
                ahora = datetime.now(timezone.utc)
                delta = ahora - inicio
                total_seg = int(delta.total_seconds())
                if total_seg < 3600:
                    return f"{total_seg // 60} min {total_seg % 60} seg"
                else:
                    return f"{total_seg // 3600} horas {(total_seg % 3600) // 60} min"
    except Exception as e:
        logger.warning(f"Error calculando tiempo de procesamiento: {e}")

    return "tiempo no disponible"


# =====================================================================
# Notificación de entrega por Telegram
# =====================================================================

async def _notificar_entrega(
    nombre_cliente: str, doc_url: str, resumen: str, tiempo: str,
) -> bool:
    """Envía notificación de entrega formateada por Telegram (HTML)."""
    # Truncar resumen a primera oración, máx 150 chars
    if resumen and len(resumen) > 150:
        punto = resumen[:150].rfind(".")
        resumen = resumen[:punto + 1] if punto > 0 else resumen[:147] + "..."

    if not resumen:
        resumen = f"Estrategia de marketing para {nombre_cliente}"

    mensaje = (
        f"✅ <b>Estrategia lista:</b> {nombre_cliente}\n\n"
        f"📄 <b>Documento:</b> {doc_url}\n\n"
        f"💡 <i>{resumen}</i>\n\n"
        f"⏱ <b>Procesamiento:</b> {tiempo}\n"
        f"🤖 <i>Ecosfera Pipeline</i>"
    )

    return await enviar_telegram(mensaje)


# =====================================================================
# Función principal
# =====================================================================

async def crear_documento(cliente_id: str, metodologia: str, db=None, cache=None) -> str:
    """
    Etapa 6: Crea Google Doc con la metodología y notifica por Telegram.

    Args:
        cliente_id: UUID del cliente
        metodologia: texto completo generado por Etapa 5
        db: instancia de DatabaseClient
        cache: instancia de RedisClient (para tiempo de procesamiento)

    Returns:
        str: URL del documento creado (webViewLink), o "" si falló
    """
    logger.info(f"cliente={cliente_id} — Iniciando creación de documento")
    advertencias = []

    if not metodologia:
        logger.warning(f"cliente={cliente_id} — Metodología vacía, no se puede crear documento")
        return ""

    # Leer datos del cliente para nombre y carpeta
    cliente = await db.obtener_cliente(cliente_id) if db else None
    nombre_cliente = cliente.get("nombre_cliente", "Cliente") if cliente else "Cliente"
    carpeta_drive_id = cliente.get("carpeta_drive_id") if cliente else None

    if not carpeta_drive_id:
        logger.error(f"cliente={cliente_id} — Sin carpeta_drive_id, no se puede crear documento")
        return ""

    # Leer resumen ejecutivo para la notificación de Telegram
    resumen = ""
    if db:
        try:
            transcript = await db.obtener_analisis_transcript(cliente_id)
            if transcript:
                resumen = transcript.get("resumen_ejecutivo", "") or ""
        except Exception:
            pass

    # Extraer secciones del texto
    secciones = extraer_secciones(metodologia)
    if not secciones:
        logger.error(f"cliente={cliente_id} — No se pudieron extraer secciones de la metodología")
        return ""

    # Crear el Google Doc
    try:
        resultado = _crear_google_doc(nombre_cliente, carpeta_drive_id, secciones)
        document_id = resultado["document_id"]
        web_view_link = resultado["web_view_link"]
        logger.info(f"cliente={cliente_id} — Documento creado: {web_view_link}")
    except Exception as e:
        logger.error(f"cliente={cliente_id} — Error creando Google Doc: {e}")
        return ""

    # Actualizar documento_drive_id en metodologias_generadas
    if db:
        try:
            metodologia_registro = await db.obtener_metodologia(cliente_id)
            if metodologia_registro:
                await db.guardar_metodologia(cliente_id, {
                    "documento_drive_id": document_id,
                    "prompt_utilizado": metodologia_registro.get("prompt_utilizado"),
                    "ia_utilizada": metodologia_registro.get("ia_utilizada"),
                    "contenido_generado": metodologia_registro.get("contenido_generado"),
                    "intentos_generacion": metodologia_registro.get("intentos_generacion", 1),
                    "secciones_completadas": metodologia_registro.get("secciones_completadas", []),
                    "estado_revision": "pendiente",
                })
            logger.info(f"cliente={cliente_id} — documento_drive_id actualizado en DB")
        except Exception as e:
            logger.warning(f"cliente={cliente_id} — Error actualizando DB (doc ya creado): {e}")
            advertencias.append(f"DB update falló: {e}")

    # Calcular tiempo de procesamiento
    tiempo = await _calcular_tiempo(cliente_id, cache)

    # Enviar notificación de entrega por Telegram
    telegram_ok = await _notificar_entrega(nombre_cliente, web_view_link, resumen, tiempo)
    if not telegram_ok:
        advertencias.append("Telegram no enviado")

    if advertencias:
        logger.warning(f"cliente={cliente_id} — Advertencias: {advertencias}")

    return web_view_link
