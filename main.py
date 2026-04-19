from pathlib import Path
import base64
import hashlib
import hmac
import io
import json
import os
import random
import secrets
import smtplib
import time
import unicodedata
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse

app = FastAPI(title="Inventario VMS")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://inventario-vms.onrender.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_EXCEL = next(BASE_DIR.glob("*.xlsx"), None)
SESSION_COOKIE = "inventario_vms_session"
DEFAULT_SECRET_KEY = "inventario-vms-session-key-2026"
SECRET_KEY = os.getenv("APP_SECRET_KEY", DEFAULT_SECRET_KEY)

df_global = pd.DataFrame()
current_file_name = DEFAULT_EXCEL.name if DEFAULT_EXCEL else ""

USERS = {
    "admin": {
        "password": "Sayayin*rey25*",
        "role": "admin",
        "display_name": "Administrador",
        "email": "",
    },
    "miriam.gamboa": {
        "password": "123456",
        "role": "tecnologia",
        "display_name": "Miriam Gamboa",
        "email": "",
    },
    "invitado": {
        "password": "lectura2026",
        "role": "invitado",
        "display_name": "Invitado",
        "email": "",
    },
}

otp_store = {}  # {username: (codigo, expira)}


def generar_otp():
    return str(random.randint(100000, 999999))


def mask_email(email: str) -> str:
    if "@" not in email:
        return email

    local, domain = email.split("@", 1)
    if len(local) <= 2:
        masked_local = f"{local[:1]}***"
    else:
        masked_local = f"{local[:2]}***"
    return f"{masked_local}@{domain}"


def is_secure_cookie_enabled() -> bool:
    return os.getenv("COOKIE_SECURE", "").strip().lower() in {"1", "true", "yes"} or bool(
        os.getenv("RENDER")
    )


def enviar_correo(destino: str, codigo: str, display_name: str) -> None:
    remitente = os.getenv("EMAIL_USER", "").strip()
    clave = os.getenv("EMAIL_PASS", "").strip()
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com").strip() or "smtp.gmail.com"
    smtp_port = int(os.getenv("SMTP_PORT", "465"))

    if not remitente or not clave:
        raise RuntimeError("Faltan EMAIL_USER o EMAIL_PASS en el entorno")

    saludo = display_name or "usuario"
    mensaje_texto = "\n".join(
        [
            f"Estimado(a) {saludo},",
            "",
            "Hemos recibido una solicitud de acceso a la plataforma Inventario VMS.",
            "",
            f"Su codigo de verificacion es: {codigo}",
            "",
            "Por seguridad, este codigo tiene una vigencia de 5 minutos y solo puede usarse una vez.",
            "",
            "Si usted no realizo esta solicitud, puede ignorar este mensaje.",
            "",
            "Atentamente,",
            "Area Sistema Tecnologia",
        ]
    )
    mensaje_html = f"""
    <html>
      <body style="margin:0; padding:24px; background-color:#f4efe6; font-family:Segoe UI, Tahoma, Arial, sans-serif; color:#2b241d;">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px; margin:0 auto; border-collapse:collapse;">
          <tr>
            <td style="padding:0;">
              <div style="background:linear-gradient(135deg, #1e6f5c 0%, #14493d 100%); border-radius:24px 24px 0 0; padding:28px 32px; color:#ffffff;">
                <div style="font-size:13px; letter-spacing:1.6px; text-transform:uppercase; opacity:0.88;">Inventario VMS</div>
                <h1 style="margin:10px 0 0; font-size:28px; line-height:1.2; font-weight:700;">Verificacion de acceso</h1>
              </div>
              <div style="background:#fffdf9; border:1px solid #e4d8c6; border-top:none; border-radius:0 0 24px 24px; padding:32px;">
                <p style="margin:0 0 16px; font-size:16px; line-height:1.7;">Estimado(a) {saludo},</p>
                <p style="margin:0 0 16px; font-size:15px; line-height:1.7; color:#5b5247;">
                  Hemos recibido una solicitud de acceso a la plataforma <strong>Inventario VMS</strong>.
                </p>
                <p style="margin:0 0 12px; font-size:15px; line-height:1.7; color:#5b5247;">
                  Utilice el siguiente codigo de verificacion para completar su ingreso:
                </p>
                <div style="margin:22px 0; padding:18px 20px; background:#f7f2ea; border:1px solid #e4d8c6; border-radius:18px; text-align:center;">
                  <div style="font-size:12px; letter-spacing:1.4px; text-transform:uppercase; color:#8b7c6b; margin-bottom:10px;">Codigo de verificacion</div>
                  <div style="font-size:34px; line-height:1; letter-spacing:8px; font-weight:700; color:#1e6f5c;">{codigo}</div>
                </div>
                <p style="margin:0 0 12px; font-size:14px; line-height:1.7; color:#5b5247;">
                  Por seguridad, este codigo tiene una vigencia de <strong>5 minutos</strong> y solo puede usarse una vez.
                </p>
                <p style="margin:0 0 24px; font-size:14px; line-height:1.7; color:#5b5247;">
                  Si usted no realizo esta solicitud, puede ignorar este mensaje.
                </p>
                <div style="padding-top:18px; border-top:1px solid #eee2d2; font-size:14px; line-height:1.7; color:#7a6f62;">
                  Atentamente,<br>
                  <strong style="color:#2b241d;">Area Sistema Tecnologia</strong>
                </div>
              </div>
            </td>
          </tr>
        </table>
      </body>
    </html>
    """
    msg = MIMEMultipart("alternative")
    msg.attach(MIMEText(mensaje_texto, "plain", "utf-8"))
    msg.attach(MIMEText(mensaje_html, "html", "utf-8"))
    msg["Subject"] = "Verificacion de acceso | Inventario VMS"
    msg["From"] = remitente
    msg["To"] = destino

    with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
        server.login(remitente, clave)
        server.send_message(msg)


def load_users_from_env() -> dict:
    raw_users = os.getenv("APP_USERS_JSON", "").strip()
    if not raw_users:
        return USERS

    try:
        parsed = json.loads(raw_users)
    except json.JSONDecodeError:
        return USERS

    loaded_users = {}
    if not isinstance(parsed, list):
        return USERS

    for item in parsed:
        if not isinstance(item, dict):
            continue
        username = str(item.get("username", "")).strip().lower()
        password = str(item.get("password", ""))
        role = str(item.get("role", "")).strip().lower()
        display_name = str(item.get("display_name", "")).strip() or username
        email = str(item.get("email", "")).strip().lower()

        if role == "ti":
            role = "tecnologia"

        if not username or not password or role not in {"admin", "tecnologia", "invitado"}:
            continue

        loaded_users[username] = {
            "password": password,
            "role": role,
            "display_name": display_name,
            "email": email,
        }

    return loaded_users or USERS


USERS = load_users_from_env()


def normalize_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""

    text = str(value).replace(" ", " ").strip()
    if text.lower() == "nan":
        return ""

    text = " ".join(text.split())
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def clean_value(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""

    text = str(value).replace(" ", " ").strip()
    if text.lower() == "nan":
        return ""

    return " ".join(text.split())


def sign_data(payload: str) -> str:
    return hmac.new(SECRET_KEY.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def create_session_token(username: str) -> str:
    payload = {
        "u": username,
        "n": secrets.token_hex(8),
    }
    payload_json = json.dumps(payload, separators=(",", ":"))
    payload_b64 = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("utf-8")
    signature = sign_data(payload_b64)
    return f"{payload_b64}.{signature}"


def read_session_token(token: str | None) -> dict | None:
    if not token or "." not in token:
        return None

    payload_b64, signature = token.rsplit(".", 1)
    if not hmac.compare_digest(sign_data(payload_b64), signature):
        return None

    try:
        payload_json = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
        payload = json.loads(payload_json)
    except Exception:
        return None

    username = payload.get("u", "")
    if username not in USERS:
        return None
    return {"username": username, **USERS[username]}


def get_current_user(request: Request) -> dict:
    token = request.cookies.get(SESSION_COOKIE)
    user = read_session_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="No autenticado")
    return user


def require_roles(request: Request, allowed_roles: set[str]) -> dict:
    user = get_current_user(request)
    if user["role"] not in allowed_roles:
        raise HTTPException(status_code=403, detail="Sin permisos para esta accion")
    return user


def safe_col(df: pd.DataFrame, index: int) -> pd.Series:
    if index < len(df.columns):
        return df.iloc[:, index].fillna("").astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype="object")


def format_date(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    formatted = parsed.dt.strftime("%d/%m/%Y")
    return formatted.fillna("")


def compact_name(parts: list[pd.Series]) -> pd.Series:
    joined = pd.concat(parts, axis=1).fillna("")
    return joined.apply(
        lambda row: " ".join(part for part in row.astype(str).map(clean_value) if part),
        axis=1,
    )


def procesar_df(df: pd.DataFrame) -> pd.DataFrame:
    raw = df.copy().fillna("")
    raw.columns = [clean_value(col) for col in raw.columns]

    processed = pd.DataFrame()
    processed["ip"] = safe_col(raw, 0).map(clean_value).str.strip()
    processed["so"] = safe_col(raw, 1).map(clean_value)
    processed["area"] = safe_col(raw, 3).map(clean_value)
    processed["centro_costo"] = safe_col(raw, 4).map(clean_value)
    processed["dni"] = safe_col(raw, 5).map(clean_value)
    processed["tipo_entorno"] = safe_col(raw, 14).map(clean_value)
    processed["hostname"] = safe_col(raw, 16).map(clean_value)
    processed["ticket"] = safe_col(raw, 23).map(clean_value)
    processed["fecha_conexion"] = format_date(safe_col(raw, 24))
    processed["fecha_asignacion"] = format_date(safe_col(raw, 25))
    processed["modelo_seguro"] = safe_col(raw, 26).map(clean_value)

    processed["nombre_completo"] = compact_name(
        [
            raw.get("1?NOMBRE", pd.Series([""] * len(raw), index=raw.index)),
            raw.get("2?NOMBRE", pd.Series([""] * len(raw), index=raw.index)),
            raw.get("1?APELLIDO", pd.Series([""] * len(raw), index=raw.index)),
            raw.get("2?APELLIDO", pd.Series([""] * len(raw), index=raw.index)),
        ]
    )

    processed["ip_limpio"] = (
         processed["ip"]
           .fillna("")
           .astype(str)
           .str.strip()
           .replace(["-", "nan", "None", "NULL"], "")
)
 
    processed["estado"] = processed["ip_limpio"].apply(
    lambda ip: "ACTIVO" if ip != "" else "CESADO"
)

    processed["modelo_seguro"] = processed["modelo_seguro"].apply(
        lambda value: "SI"
        if normalize_text(value) == "si"
        else ("NO" if normalize_text(value) == "no" else "")
    )
    processed["es_agente_nuevo"] = processed["modelo_seguro"].apply(
        lambda value: "SI" if value == "NO" else "NO"
    )

    processed["search_blob"] = processed.apply(
        lambda row: " ".join(
            normalize_text(row[field])
            for field in [
                "dni",
                "nombre_completo",
                "ip",
                "tipo_entorno",
                "hostname",
                "ticket",
                "area",
                "centro_costo",
                "estado",
                "fecha_conexion",
                "fecha_asignacion",
            ]
        ),
        axis=1,
    )

    return processed.fillna("")


def load_dataframe_from_excel(file_source) -> pd.DataFrame:
    df = pd.read_excel(file_source, dtype=str)
    return procesar_df(df)


def ensure_data_loaded() -> pd.DataFrame:
    global df_global
    if not df_global.empty:
        return df_global

    if DEFAULT_EXCEL and DEFAULT_EXCEL.exists():
        df_global = load_dataframe_from_excel(DEFAULT_EXCEL)

    return df_global


def filter_by_status(df: pd.DataFrame, status: str) -> pd.DataFrame:
    normalized = normalize_text(status)
    if normalized in {"activo", "activos"}:
        return df[df["estado"] == "ACTIVO"]
    if normalized in {"cesado", "cesados"}:
        return df[df["estado"] == "CESADO"]
    return df


def smart_search(df: pd.DataFrame, query: str) -> pd.DataFrame:
    normalized_query = normalize_text(query)
    if not normalized_query:
        return df

    raw_terms = [chunk.strip() for chunk in normalized_query.split(",") if chunk.strip()]
    terms = []
    for chunk in raw_terms:
        parts = [term for term in chunk.split() if term]
        if parts:
            terms.append(parts)

    if not terms:
        return df

    scores = pd.Series(0, index=df.index, dtype="int64")

    normalized_fields = {
        field: df[field].map(normalize_text)
        for field in ["dni", "ip", "hostname", "ticket", "area", "centro_costo", "so"]
    }

    for group in terms:
        group_score = pd.Series(0, index=df.index, dtype="int64")
        for term in group:
            contains = df["search_blob"].str.contains(term, na=False)
            group_score += contains.astype(int)

            for field_values in normalized_fields.values():
                group_score += (field_values == term).astype(int) * 4
                group_score += field_values.str.startswith(term, na=False).astype(int) * 2

        scores += (group_score > 0).astype(int) * 5
        scores += group_score

    result = df[scores > 0].copy()
    result["score"] = scores[scores > 0]
    return result.sort_values(
        by=["score", "estado", "fecha_asignacion", "ticket"],
        ascending=[False, True, False, True],
    )


def summarize_group(df: pd.DataFrame, field: str, limit: int = 8) -> list[dict]:
    subset = df[df[field] != ""]
    if subset.empty:
        return []

    grouped = (
        subset.groupby(field)
        .size()
        .reset_index(name="cantidad")
        .sort_values(by=["cantidad", field], ascending=[False, True])
        .head(limit)
    )
    return grouped.to_dict(orient="records")


def parse_display_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, format="%d/%m/%Y", errors="coerce")


def ticket_summary(df: pd.DataFrame, limit: int = 12) -> list[dict]:
    subset = df[df["ticket"] != ""].copy()
    if subset.empty:
        return []

    # 🔥 limpiar IP
    subset["ip_limpio"] = (
    subset["ip"]
    .fillna("")
    .astype(str)
    .str.strip()
    .replace(["-", "nan", "None", "NULL"], "")
)

    # 🔹 solicitudes (FILAS)
    solicitudes = (
        subset.groupby("ticket")
        .size()
        .reset_index(name="solicitudes")
    )

    # 🔹 activos (filas con IP)
    activos = (
        subset[subset["ip_limpio"] != ""]
        .groupby("ticket")
        .size()
        .reset_index(name="activos")
    )

    # 🔹 cesados (filas sin IP)
    cesados = (
        subset[subset["ip_limpio"] == ""]
        .groupby("ticket")
        .size()
        .reset_index(name="cesados")
    )

    # 🔹 modelo seguro SI
    modelo_si = (
        subset[subset["modelo_seguro"] == "SI"]
        .groupby("ticket")
        .size()
        .reset_index(name="modelo_seguro_si")
    )

    # 🔹 modelo seguro NO
    modelo_no = (
        subset[subset["modelo_seguro"] == "NO"]
        .groupby("ticket")
        .size()
        .reset_index(name="personal_nuevo_no")
    )

    # 🔹 merge
    summary = solicitudes \
        .merge(activos, on="ticket", how="left") \
        .merge(cesados, on="ticket", how="left") \
        .merge(modelo_si, on="ticket", how="left") \
        .merge(modelo_no, on="ticket", how="left")

    summary = summary.fillna(0)

    # 🔹 fechas
    subset["fecha_conexion_dt"] = parse_display_dates(subset["fecha_conexion"])
    subset["fecha_asignacion_dt"] = parse_display_dates(subset["fecha_asignacion"])

    fechas = (
        subset.groupby("ticket")
        .agg(
            fecha_conexion=("fecha_conexion_dt", "max"),
            fecha_asignacion=("fecha_asignacion_dt", "max"),
        )
        .reset_index()
    )

    summary = summary.merge(fechas, on="ticket", how="left")

    summary["fecha_conexion"] = summary["fecha_conexion"].dt.strftime("%d/%m/%Y").fillna("")
    summary["fecha_asignacion"] = summary["fecha_asignacion"].dt.strftime("%d/%m/%Y").fillna("")

    return (
        summary.sort_values(by=["solicitudes", "activos"], ascending=[False, False])
        .head(limit)
        .to_dict(orient="records")
    )


def classify_assignment(row: pd.Series) -> str:
    if clean_value(row.get("ip", "")) == "":
        return "SIN_IP"

    area = normalize_text(row.get("area", ""))
    centro = normalize_text(row.get("centro_costo", ""))
    combined = f"{area} {centro}".strip()

    if "sede camana" in combined:
        return "SEDE_CAMANA"
    if "sede chota" in combined or "sedechota" in combined:
        return "SEDE_CHOTA"
    if (
        "sede centro civico" in combined
        or "sede civico" in combined
        or "centrocivico" in combined
        or "centro civico" in combined
    ):
        return "SEDE_CENTRO_CIVICO"

    excluded_tags = [
        "falla remoto",
        "tv",
        "capacitacion",
        "pruebas",
        "highend",
        "pivot",
        "ciberseguridad",
    ]
    if any(tag in combined for tag in excluded_tags):
        return "EXCLUIDO"

    return "ASIGNADO_SERVICIO"


def filter_by_tipo_entorno(df: pd.DataFrame, tipo_entorno: str) -> pd.DataFrame:
    tipo_normalized = normalize_text(tipo_entorno)
    if tipo_normalized not in {"ts", "vms", "vm", "vmm"}:
        return df

    normalized_series = df["tipo_entorno"].map(normalize_text)
    if tipo_normalized == "ts":
        mask = normalized_series.str.contains("terminal server", na=False) | normalized_series.eq("ts")
        return df[mask]

    mask = (
        normalized_series.str.contains("vm", na=False)
        | normalized_series.str.contains("vmm", na=False)
        | normalized_series.eq("vms")
    )
    return df[mask]


def remote_assignments_only(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    scoped = df.copy()
    scoped = scoped[(scoped["ip"] != "") & (scoped["dni"] != "")]
    if scoped.empty:
        return scoped

    scoped["clasificacion_asignacion"] = scoped.apply(classify_assignment, axis=1)
    return scoped[scoped["clasificacion_asignacion"] == "ASIGNADO_SERVICIO"].copy()


def build_search_dashboard(df: pd.DataFrame) -> dict:
    scoped = remote_assignments_only(df)
    return {
        "total_asignaciones_remotas": int(len(scoped)),
        "ips_unicas": int(scoped["ip"].nunique()) if not scoped.empty else 0,
        "usuarios_con_dni": int(scoped["dni"].nunique()) if not scoped.empty else 0,
        "por_area": summarize_group(scoped, "area", limit=12),
        "por_centro_costo": summarize_group(scoped, "centro_costo", limit=12),
    }


def build_assignment_pivot(df: pd.DataFrame, limit: int = 200) -> list[dict]:
    scoped = remote_assignments_only(df)
    if scoped.empty:
        return []

    scoped = scoped.copy()
    normalized_so = scoped["so"].map(normalize_text)
    scoped["win_10"] = normalized_so.str.contains("windows 10", na=False).astype(int)
    scoped["win_11"] = normalized_so.str.contains("windows 11", na=False).astype(int)

    grouped = (
        scoped.groupby(["area", "centro_costo"], dropna=False)
        .agg(
            cantidad_ips=("ip", "size"),
            ips_unicas=("ip", "nunique"),
            usuarios_dni=("dni", "nunique"),
            tickets_unicos=("ticket", lambda s: int(s.replace("", pd.NA).dropna().nunique())),
            windows_10=("win_10", "sum"),
            windows_11=("win_11", "sum"),
        )
        .reset_index()
        .sort_values(
            by=["cantidad_ips", "ips_unicas", "usuarios_dni", "area", "centro_costo"],
            ascending=[False, False, False, True, True],
        )
        .head(limit)
    )
    return grouped.to_dict(orient="records")


def build_remote_assignments_export(df: pd.DataFrame) -> pd.DataFrame:
    scoped = remote_assignments_only(df).copy()
    if scoped.empty:
        return pd.DataFrame(
            columns=[
                "estado",
                "tipo_entorno",
                "arquitectura",
                "dni",
                "nombre_completo",
                "ip",
                "hostname",
                "ticket",
                "area",
                "centro_costo",
                "fecha_conexion",
                "fecha_asignacion",
                "modelo_seguro",
            ]
        )

    export_df = pd.DataFrame(
        {
            "estado": scoped["estado"],
            "tipo_entorno": scoped["tipo_entorno"],
            "arquitectura": scoped["so"],
            "dni": scoped["dni"],
            "nombre_completo": scoped["nombre_completo"],
            "ip": scoped["ip"],
            "hostname": scoped["hostname"],
            "ticket": scoped["ticket"],
            "area": scoped["area"],
            "centro_costo": scoped["centro_costo"],
            "fecha_conexion": scoped["fecha_conexion"],
            "fecha_asignacion": scoped["fecha_asignacion"],
            "modelo_seguro": scoped["modelo_seguro"],
        }
    )
    return export_df


def build_standard_export(df: pd.DataFrame) -> pd.DataFrame:
    scoped = df.copy()
    return pd.DataFrame(
        {
            "estado": scoped["estado"] if "estado" in scoped else "",
            "tipo_entorno": scoped["tipo_entorno"] if "tipo_entorno" in scoped else "",
            "arquitectura": scoped["so"] if "so" in scoped else "",
            "dni": scoped["dni"] if "dni" in scoped else "",
            "nombre_completo": scoped["nombre_completo"] if "nombre_completo" in scoped else "",
            "ip": scoped["ip"] if "ip" in scoped else "",
            "hostname": scoped["hostname"] if "hostname" in scoped else "",
            "ticket": scoped["ticket"] if "ticket" in scoped else "",
            "area": scoped["area"] if "area" in scoped else "",
            "centro_costo": scoped["centro_costo"] if "centro_costo" in scoped else "",
            "fecha_conexion": scoped["fecha_conexion"] if "fecha_conexion" in scoped else "",
            "fecha_asignacion": scoped["fecha_asignacion"] if "fecha_asignacion" in scoped else "",
            "modelo_seguro": scoped["modelo_seguro"] if "modelo_seguro" in scoped else "",
        }
    )


def build_ticket_audit(ticket: str, tipo_entorno: str = "todos") -> dict:
    cleaned_ticket = clean_value(ticket)
    if not cleaned_ticket:
        return {"ticket": "", "resumen": {}, "filas": []}

    df = filter_by_tipo_entorno(ensure_data_loaded(), tipo_entorno)
    scoped = df[df["ticket"] == cleaned_ticket].copy()
    if scoped.empty:
        return {
            "ticket": cleaned_ticket,
            "resumen": {
                "filas_totales": 0,
                "solicitudes_ticket_dni": 0,
                "activos": 0,
                "cesados": 0,
            },
            "filas": [],
        }

    scoped["cuenta_solicitud"] = ((scoped["ticket"] != "") & (scoped["dni"] != "")).astype(int)
    scoped["cuenta_activo"] = ((scoped["ticket"] != "") & (scoped["dni"] != "") & (scoped["ip"] != "")).astype(int)
    scoped["cuenta_cesado"] = ((scoped["ticket"] != "") & (scoped["dni"] != "") & (scoped["ip"] == "")).astype(int)
    scoped["modelo_seguro_activo_si"] = (
        (scoped["modelo_seguro"] == "SI") & (scoped["ticket"] != "") & (scoped["dni"] != "") & (scoped["ip"] != "")
    ).astype(int)
    scoped["modelo_seguro_activo_no"] = (
        (scoped["modelo_seguro"] == "NO") & (scoped["ticket"] != "") & (scoped["dni"] != "") & (scoped["ip"] != "")
    ).astype(int)
    scoped["motivo_conteo"] = scoped.apply(
        lambda row: "ACTIVO"
        if int(row["cuenta_activo"]) == 1
        else ("CESADO" if int(row["cuenta_cesado"]) == 1 else "NO CUENTA"),
        axis=1,
    )

    filas = build_standard_export(scoped)
    filas["cuenta_solicitud"] = scoped["cuenta_solicitud"].astype(int).values
    filas["cuenta_activo"] = scoped["cuenta_activo"].astype(int).values
    filas["cuenta_cesado"] = scoped["cuenta_cesado"].astype(int).values
    filas["motivo_conteo"] = scoped["motivo_conteo"].values

    resumen = {
        "filas_totales": int(len(scoped)),
        "solicitudes_ticket_dni": int(((scoped["ticket"] != "") & (scoped["dni"] != "")).sum()),
        "activos": int(scoped["cuenta_activo"].sum()),
        "cesados": int(scoped["cuenta_cesado"].sum()),
        "modelo_seguro_si_activo": int(scoped["modelo_seguro_activo_si"].sum()),
        "modelo_seguro_no_activo": int(scoped["modelo_seguro_activo_no"].sum()),
    }

    return {
        "ticket": cleaned_ticket,
        "resumen": resumen,
        "filas": filas.to_dict(orient="records"),
    }


def get_dashboard_scoped_df(tipo_entorno: str, status: str) -> pd.DataFrame:
    df = filter_by_status(ensure_data_loaded(), status)
    return filter_by_tipo_entorno(df, tipo_entorno)


def get_search_scoped_df(q: str, tipo_entorno: str, status: str) -> pd.DataFrame:
    df = get_dashboard_scoped_df(tipo_entorno, status)
    return smart_search(df, q) if q else df.copy()


def get_card_export_dataframe(segment: str, q: str, tipo_entorno: str, status: str) -> pd.DataFrame:
    dashboard_df = get_dashboard_scoped_df(tipo_entorno, status)
    search_df = get_search_scoped_df(q, tipo_entorno, status)

    if segment == "asignados_servicio":
        scoped = dashboard_df[dashboard_df["ip"] != ""].copy()
        if not scoped.empty:
            scoped["clasificacion_asignacion"] = scoped.apply(classify_assignment, axis=1)
            scoped = scoped[scoped["clasificacion_asignacion"] == "ASIGNADO_SERVICIO"]
        return build_standard_export(scoped)

    if segment == "sede_camana":
        scoped = dashboard_df[dashboard_df["ip"] != ""].copy()
        if not scoped.empty:
            scoped["clasificacion_asignacion"] = scoped.apply(classify_assignment, axis=1)
            scoped = scoped[scoped["clasificacion_asignacion"] == "SEDE_CAMANA"]
        return build_standard_export(scoped)

    if segment == "sede_chota":
        scoped = dashboard_df[dashboard_df["ip"] != ""].copy()
        if not scoped.empty:
            scoped["clasificacion_asignacion"] = scoped.apply(classify_assignment, axis=1)
            scoped = scoped[scoped["clasificacion_asignacion"] == "SEDE_CHOTA"]
        return build_standard_export(scoped)

    if segment == "sede_centro_civico":
        scoped = dashboard_df[dashboard_df["ip"] != ""].copy()
        if not scoped.empty:
            scoped["clasificacion_asignacion"] = scoped.apply(classify_assignment, axis=1)
            scoped = scoped[scoped["clasificacion_asignacion"] == "SEDE_CENTRO_CIVICO"]
        return build_standard_export(scoped)

    if segment == "activos_excluidos":
        scoped = dashboard_df[dashboard_df["ip"] != ""].copy()
        if not scoped.empty:
            scoped["clasificacion_asignacion"] = scoped.apply(classify_assignment, axis=1)
            scoped = scoped[scoped["clasificacion_asignacion"] == "EXCLUIDO"]
        return build_standard_export(scoped)

    if segment == "resultados":
        return build_standard_export(search_df)

    if segment == "total_registros":
        return build_standard_export(dashboard_df)

    if segment == "total_activos":
        return build_standard_export(dashboard_df[dashboard_df["estado"] == "ACTIVO"])

    if segment == "total_cesados":
        return build_standard_export(dashboard_df[dashboard_df["estado"] == "CESADO"])

    if segment == "search_asignaciones_remotas":
        return build_remote_assignments_export(search_df)

    return pd.DataFrame()


@app.get("/")
def serve_index():
    return FileResponse(BASE_DIR / "index.html")


@app.post("/login")
async def login(request: Request):
    body = await request.json()
    username = str(body.get("username", "")).strip().lower()
    password = str(body.get("password", ""))

    user = USERS.get(username)
    if not user or user["password"] != password:
        raise HTTPException(status_code=401, detail="Usuario o contrasena incorrecta")

    email = clean_value(user.get("email", "")).lower()
    if not email or "@" not in email:
        raise HTTPException(
            status_code=400,
            detail="Este usuario no tiene un correo configurado para verificacion",
        )

    codigo = generar_otp()
    expira = time.time() + 300
    otp_store[username] = (codigo, expira)

    try:
        enviar_correo(email, codigo, user.get("display_name", username))
    except Exception:
        otp_store.pop(username, None)
        raise HTTPException(
            status_code=500,
            detail="No se pudo enviar el codigo al correo configurado",
        )

    return {
        "step": "otp",
        "username": username,
        "email_hint": mask_email(email),
        "expires_in": 300,
    }


@app.post("/logout")
def logout(response: Response):
    response.delete_cookie(SESSION_COOKIE, path="/")
    return {"ok": True}


@app.get("/me")
def me(request: Request):
    user = get_current_user(request)
    return {
        "username": user["username"],
        "role": user["role"],
        "display_name": user["display_name"],
    }

@app.post("/verify-otp")
async def verify_otp(request: Request, response: Response):
    body = await request.json()
    username = str(body.get("username", "")).strip().lower()
    codigo = str(body.get("codigo", "")).strip()

    if username not in otp_store:
        raise HTTPException(status_code=400, detail="No hay codigo")

    codigo_guardado, expira = otp_store[username]

    if time.time() > expira:
        otp_store.pop(username, None)
        raise HTTPException(status_code=400, detail="Codigo expirado")

    if codigo != codigo_guardado:
        raise HTTPException(status_code=400, detail="Codigo incorrecto")

    otp_store.pop(username, None)
    token = create_session_token(username)
    response.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        httponly=True,
        samesite="lax",
        secure=is_secure_cookie_enabled(),
        path="/",
    )

    user = USERS[username]

    return {
        "username": username,
        "role": user["role"],
        "display_name": user["display_name"],
    }

@app.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...)):
    require_roles(request, {"admin", "tecnologia"})
    global df_global, current_file_name
    df_global = load_dataframe_from_excel(file.file)
    current_file_name = file.filename or "archivo_subido.xlsx"
    return {
        "mensaje": "Archivo cargado correctamente",
        "registros": int(len(df_global)),
        "archivo": current_file_name,
    }


@app.get("/vms")
def get_vms(
    request: Request,
    q: str = Query(default=""),
    tipo_entorno: str = Query(default="todos"),
    status: str = Query(default="todos"),
    limit: int = Query(default=200, ge=1, le=5000),
):
    get_current_user(request)
    df = filter_by_status(ensure_data_loaded(), status)
    df = filter_by_tipo_entorno(df, tipo_entorno)
    result = smart_search(df, q) if q else df.copy()
    result = result.drop(columns=["search_blob"], errors="ignore").head(limit)
    return result.fillna("").to_dict(orient="records")


@app.get("/dashboard")
def dashboard(
    request: Request,
    status: str = Query(default="todos"),
    tipo_entorno: str = Query(default="todos"),
):
    get_current_user(request)

    base_df = ensure_data_loaded()  # ✅ alineado

    # 🔹 cards (filtrados)
    df = filter_by_status(base_df, status)
    df = filter_by_tipo_entorno(df, tipo_entorno)

    # 🔥 ticket summary SIN filtro de estado
    ticket_df = filter_by_tipo_entorno(base_df, tipo_entorno)

    total = len(df)
    activos = int((df["estado"] == "ACTIVO").sum()) if not df.empty else 0
    cesados = int((df["estado"] == "CESADO").sum()) if not df.empty else 0

  
    tickets_con_ip = int(df.loc[df["ip"] != "", "ticket"].replace("", pd.NA).dropna().nunique())
    activos_con_ip = df[df["ip"] != ""].copy()
    if not activos_con_ip.empty:
        activos_con_ip["clasificacion_asignacion"] = activos_con_ip.apply(classify_assignment, axis=1)
    else:
        activos_con_ip["clasificacion_asignacion"] = pd.Series(dtype="object")

    asignados_servicio = int((activos_con_ip["clasificacion_asignacion"] == "ASIGNADO_SERVICIO").sum())
    sede_camana = int((activos_con_ip["clasificacion_asignacion"] == "SEDE_CAMANA").sum())
    sede_chota = int((activos_con_ip["clasificacion_asignacion"] == "SEDE_CHOTA").sum())
    sede_centro_civico = int(
        (activos_con_ip["clasificacion_asignacion"] == "SEDE_CENTRO_CIVICO").sum()
    )
    excluidos = int((activos_con_ip["clasificacion_asignacion"] == "EXCLUIDO").sum())

    return {
    "archivo": current_file_name,
    "total_registros": total,
    "total_activos": activos,
    "total_cesados": cesados,
    "asignados_servicio": asignados_servicio,
    "sede_camana": sede_camana,
    "sede_chota": sede_chota,
    "sede_centro_civico": sede_centro_civico,
    "activos_excluidos": excluidos,
    "tickets_unicos": int(df.loc[df["ticket"] != "", "ticket"].nunique()) if not df.empty else 0,
    "tickets_con_ip": tickets_con_ip,
    "por_area": summarize_group(df, "area"),
    "por_centro_costo": summarize_group(df, "centro_costo"),
    "por_ticket": ticket_summary(ticket_df),
}


@app.get("/search-dashboard")
def search_dashboard(
    request: Request,
    q: str = Query(default=""),
    tipo_entorno: str = Query(default="todos"),
    status: str = Query(default="todos"),
):
    get_current_user(request)

    df = filter_by_status(ensure_data_loaded(), status)
    df = filter_by_tipo_entorno(df, tipo_entorno)

    result = smart_search(df, q) if q else df.copy()

    data = build_search_dashboard(result)

    # 🔥 AQUÍ VA
    data["por_ticket"] = ticket_summary(result)

    return data


@app.get("/search-pivot")
def search_pivot(
    request: Request,
    q: str = Query(default=""),
    tipo_entorno: str = Query(default="todos"),
    status: str = Query(default="todos"),
    limit: int = Query(default=200, ge=1, le=1000),
):
    get_current_user(request)
    df = filter_by_status(ensure_data_loaded(), status)
    df = filter_by_tipo_entorno(df, tipo_entorno)
    result = smart_search(df, q) if q else df.copy()
    return build_assignment_pivot(result, limit=limit)


@app.get("/export-search-assignments")
def export_search_assignments(
    request: Request,
    q: str = Query(default=""),
    tipo_entorno: str = Query(default="todos"),
    status: str = Query(default="todos"),
):
    require_roles(request, {"admin"})
    df = filter_by_status(ensure_data_loaded(), status)
    df = filter_by_tipo_entorno(df, tipo_entorno)
    result = smart_search(df, q) if q else df.copy()
    export_df = build_remote_assignments_export(result)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="asignaciones_remotas")
    output.seek(0)

    filename = "asignaciones_remotas_filtradas.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@app.get("/export-card")
def export_card(
    request: Request,
    segment: str = Query(...),
    q: str = Query(default=""),
    tipo_entorno: str = Query(default="todos"),
    status: str = Query(default="todos"),
):
    require_roles(request, {"admin"})
    export_df = get_card_export_dataframe(segment, q, tipo_entorno, status)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="datos")
    output.seek(0)

    safe_segment = normalize_text(segment).replace(" ", "_") or "export"
    filename = f"{safe_segment}.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@app.get("/ticket-audit")
def ticket_audit(
    request: Request,
    ticket: str = Query(...),
    tipo_entorno: str = Query(default="todos"),
):
    get_current_user(request)
    return build_ticket_audit(ticket, tipo_entorno)


@app.get("/meta")
def meta():
    df = ensure_data_loaded()
    return {
        "archivo": current_file_name,
        "total_registros": int(len(df)),
        "columnas_clave": [
            "ip",
            "dni",
            "nombre_completo",
            "tipo_entorno",
            "hostname",
            "ticket",
            "area",
            "centro_costo",
            "fecha_conexion",
            "fecha_asignacion",
            "estado",
            "modelo_seguro",
        ],
        "excel_por_defecto": DEFAULT_EXCEL.name if DEFAULT_EXCEL else "",
    }


from fastapi.responses import Response

@app.get("/health")
@app.head("/health")
def health():
    return Response(status_code=200)
