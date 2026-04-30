from pathlib import Path
import base64
import hashlib
import hmac
import io
import json
import os
import random
import re
import secrets
import smtplib
import time
import unicodedata
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from fastapi import FastAPI, File, Header, HTTPException, Query, Request, Response, UploadFile
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
SESSION_TIMEOUT_SECONDS = int(os.getenv("SESSION_TIMEOUT_SECONDS", str(2 * 60 * 60)))
PASSWORD_MAX_AGE_SECONDS = int(os.getenv("PASSWORD_MAX_AGE_SECONDS", str(90 * 24 * 60 * 60)))
DEFAULT_SECRET_KEY = "inventario-vms-session-key-2026"
SECRET_KEY = os.getenv("APP_SECRET_KEY", DEFAULT_SECRET_KEY)

df_global = pd.DataFrame()
current_file_name = DEFAULT_EXCEL.name if DEFAULT_EXCEL else ""
infra_df_global = pd.DataFrame()
infra_file_name = ""
applications_df_global = pd.DataFrame()

DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data")))
USERS_STORE_PATH = DATA_DIR / "users.json"
INVITES_STORE_PATH = DATA_DIR / "invites.json"
APPLICATIONS_STORE_PATH = DATA_DIR / "applications_tto.json"
AGENT_REPORT_TOKEN = os.getenv("AGENT_REPORT_TOKEN", "").strip()
ENTRY_ACCESS_TOKEN = os.getenv("ENTRY_ACCESS_TOKEN", "").strip()
SMTP_TIMEOUT_SECONDS = int(os.getenv("SMTP_TIMEOUT_SECONDS", "20"))
SMTP_SECURITY = os.getenv("SMTP_SECURITY", "ssl").strip().lower()
SHOW_MAIL_ERROR_DETAILS = os.getenv("SHOW_MAIL_ERROR_DETAILS", "").strip().lower() in {"1", "true", "yes"}
LOGIN_OTP_ENABLED = os.getenv("LOGIN_OTP_ENABLED", "true").strip().lower() in {"1", "true", "yes"}

USERS = {
    "admin": {
        "password": "Sayayin*rey25*",
        "role": "admin",
        "display_name": "Administrador",
        "email": "",
        "email_greeting": "",
    },
    "miriam.gamboa": {
        "password": "123456",
        "role": "tecnologia",
        "display_name": "Miriam Gamboa",
        "email": "",
        "email_greeting": "",
    },
    "invitado": {
        "password": "lectura2026",
        "role": "invitado",
        "display_name": "Invitado",
        "email": "",
        "email_greeting": "",
    },
}

PASSWORD_POLICY = {
    "min_length": 10,
    "require_upper": True,
    "require_lower": True,
    "require_digit": True,
    "require_symbol": True,
}

ROLE_DEFAULT_PERMISSIONS = {
    "admin": ["inventario", "dashboard_vms", "aplicaciones_tto", "invitaciones", "exportar", "cargar_excel"],
    "tecnologia": ["inventario", "dashboard_vms", "aplicaciones_tto", "cargar_excel"],
    "invitado": ["inventario"],
}

otp_store = {}  # {username: (codigo, expira)}
invite_store = {}
password_reset_store = {}
EXCLUDED_ASSIGNMENT_TAGS = [
    "no existe",
    "dotacion",
    "falla remoto",
    "capacitacion",
    "tv",
    "sede camana",
    "sedechota",
    "sede chota",
    "sede centro civico",
    "sede civico",
    "centro civico",
    "centrocivico",
    "pivot",
]


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


def now_ts() -> int:
    return int(time.time())


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_json_file(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json_file(path: Path, payload) -> None:
    ensure_data_dir()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def send_smtp_message(msg: MIMEMultipart) -> None:
    remitente = os.getenv("EMAIL_USER", "").strip()
    clave = os.getenv("EMAIL_PASS", "").strip()
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com").strip() or "smtp.gmail.com"
    default_port = "587" if SMTP_SECURITY in {"starttls", "tls"} else "465"
    smtp_port = int(os.getenv("SMTP_PORT", default_port))

    if not remitente or not clave:
        raise RuntimeError("Faltan EMAIL_USER o EMAIL_PASS en el entorno")

    if SMTP_SECURITY in {"starttls", "tls"}:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=SMTP_TIMEOUT_SECONDS) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(remitente, clave)
            server.send_message(msg)
        return

    with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=SMTP_TIMEOUT_SECONDS) as server:
        server.login(remitente, clave)
        server.send_message(msg)


def hash_password(password: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 150000)
    return f"pbkdf2_sha256${salt}${digest.hex()}"


def verify_password(stored: str, password: str) -> bool:
    if stored.startswith("pbkdf2_sha256$"):
        try:
            _, salt, expected = stored.split("$", 2)
        except ValueError:
            return False
        return hmac.compare_digest(hash_password(password, salt), stored)
    return hmac.compare_digest(stored, password)


def validate_password_policy(password: str, username: str = "", email: str = "", display_name: str = "") -> list[str]:
    errors = []
    if len(password) < PASSWORD_POLICY["min_length"]:
        errors.append("Debe tener al menos 10 caracteres.")
    if PASSWORD_POLICY["require_upper"] and not re.search(r"[A-Z]", password):
        errors.append("Debe incluir una letra mayuscula.")
    if PASSWORD_POLICY["require_lower"] and not re.search(r"[a-z]", password):
        errors.append("Debe incluir una letra minuscula.")
    if PASSWORD_POLICY["require_digit"] and not re.search(r"\d", password):
        errors.append("Debe incluir un numero.")
    if PASSWORD_POLICY["require_symbol"] and not re.search(r"[^A-Za-z0-9]", password):
        errors.append("Debe incluir un simbolo.")

    normalized_password = normalize_text(password)
    blocked_parts = [
        normalize_text(username),
        normalize_text(email.split("@", 1)[0] if "@" in email else email),
    ]
    blocked_parts.extend(part for part in normalize_text(display_name).split() if len(part) >= 4)
    if any(part and len(part) >= 4 and part in normalized_password for part in blocked_parts):
        errors.append("No debe contener datos evidentes del usuario.")
    return errors

def normalize_permissions(raw_permissions, role: str) -> list[str]:
    if isinstance(raw_permissions, str):
        permissions = [item.strip() for item in raw_permissions.split(",")]
    elif isinstance(raw_permissions, list):
        permissions = [str(item).strip() for item in raw_permissions]
    else:
        permissions = ROLE_DEFAULT_PERMISSIONS.get(role, ["inventario"])

    allowed = set().union(*[set(items) for items in ROLE_DEFAULT_PERMISSIONS.values()])
    clean_permissions = []
    for permission in permissions:
        normalized = re.sub(r"[^a-z0-9]+", "", str(permission).strip().lower())
        mapped = {
            "dashboard": "dashboard_vms",
            "dashboardvms": "dashboard_vms",
            "aplicacionestto": "aplicaciones_tto",
            "apps": "aplicaciones_tto",
            "applications": "aplicaciones_tto",
            "invitacion": "invitaciones",
            "invitaciones": "invitaciones",
            "inventario": "inventario",
            "inventariovms": "inventario",
            "exportar": "exportar",
            "cargarexcel": "cargar_excel",
        }.get(normalized, permission)
        if mapped in allowed and mapped not in clean_permissions:
            clean_permissions.append(mapped)
    return clean_permissions or ROLE_DEFAULT_PERMISSIONS.get(role, ["inventario"])


def user_permissions(user: dict) -> list[str]:
    return normalize_permissions(user.get("permissions"), user.get("role", "invitado"))


def has_permission(user: dict, permission: str) -> bool:
    return permission in user_permissions(user)


def make_action_token(kind: str, subject: str, ttl_seconds: int = 24 * 60 * 60) -> str:
    payload = {
        "k": kind,
        "s": subject,
        "n": secrets.token_urlsafe(18),
        "iat": now_ts(),
        "exp": now_ts() + ttl_seconds,
    }
    payload_json = json.dumps(payload, separators=(",", ":"))
    payload_b64 = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("utf-8")
    return f"{payload_b64}.{sign_data(payload_b64)}"


def read_action_token(token: str, expected_kind: str) -> dict | None:
    session = read_session_token(token)
    if session:
        return None
    if not token or "." not in token:
        return None
    payload_b64, signature = token.rsplit(".", 1)
    if not hmac.compare_digest(sign_data(payload_b64), signature):
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8"))
    except Exception:
        return None
    if payload.get("k") != expected_kind or now_ts() > int(payload.get("exp", 0)):
        return None
    return payload


def enviar_correo(destino: str, codigo: str, display_name: str, email_greeting: str = "") -> None:
    remitente = os.getenv("EMAIL_USER", "").strip()

    if not remitente:
        raise RuntimeError("Faltan EMAIL_USER o EMAIL_PASS en el entorno")

    saludo = clean_value(email_greeting) or clean_value(display_name) or "usuario"
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

    send_smtp_message(msg)


def enviar_correo_html(destino: str, asunto: str, texto: str, html: str) -> None:
    remitente = os.getenv("EMAIL_USER", "").strip()

    if not remitente:
        raise RuntimeError("Faltan EMAIL_USER o EMAIL_PASS en el entorno")

    msg = MIMEMultipart("alternative")
    msg.attach(MIMEText(texto, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))
    msg["Subject"] = asunto
    msg["From"] = remitente
    msg["To"] = destino

    send_smtp_message(msg)


def build_public_url(request: Request, token: str, mode: str) -> str:
    base_url = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
    if not base_url:
        base_url = str(request.base_url).rstrip("/")
    return f"{base_url}/?{mode}={token}"


def find_user_by_email(email: str) -> tuple[str, dict] | tuple[str, None]:
    normalized_email = email.strip().lower()
    for username, user in USERS.items():
        if str(user.get("email", "")).strip().lower() == normalized_email:
            return username, user
    return "", None


def send_link_email(destino: str, asunto: str, titulo: str, descripcion: str, link: str) -> None:
    texto = "\n".join([titulo, "", descripcion, "", link, "", "Area Sistema Tecnologia"])
    html = f"""
    <html>
      <body style="margin:0; padding:24px; background:#f4efe6; font-family:Segoe UI,Tahoma,Arial,sans-serif; color:#2b241d;">
        <div style="max-width:640px; margin:0 auto; background:#fffdf9; border:1px solid #e4d8c6; border-radius:22px; overflow:hidden;">
          <div style="background:#1e6f5c; color:#fff; padding:24px 28px;">
            <div style="font-size:13px; letter-spacing:1.4px; text-transform:uppercase;">Inventario VMS</div>
            <h1 style="margin:8px 0 0; font-size:26px;">{titulo}</h1>
          </div>
          <div style="padding:28px;">
            <p style="font-size:15px; line-height:1.7; color:#5b5247;">{descripcion}</p>
            <p style="margin:24px 0;">
              <a href="{link}" style="display:inline-block; background:#1e6f5c; color:#fff; padding:13px 18px; border-radius:12px; text-decoration:none; font-weight:700;">Abrir enlace seguro</a>
            </p>
            <p style="font-size:13px; line-height:1.6; color:#7a6f62;">Si el boton no abre, copie este enlace:<br>{link}</p>
            <div style="padding-top:18px; border-top:1px solid #eee2d2; font-size:14px; color:#7a6f62;">Area Sistema Tecnologia</div>
          </div>
        </div>
      </body>
    </html>
    """
    enviar_correo_html(destino, asunto, texto, html)


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
        password_hash = str(item.get("password_hash", ""))
        role = str(item.get("role", "")).strip().lower()
        display_name = str(item.get("display_name", "")).strip() or username
        email = str(item.get("email", "")).strip().lower()
        email_greeting = str(item.get("email_greeting", "")).strip()
        password_changed_at = int(item.get("password_changed_at", 0) or 0)
        force_password_change = bool(item.get("force_password_change", role in {"tecnologia", "invitado"}))
        permissions = normalize_permissions(item.get("permissions"), role)

        if role == "ti":
            role = "tecnologia"

        if not username or (not password and not password_hash) or role not in {"admin", "tecnologia", "invitado"}:
            continue

        loaded_users[username] = {
            "password": password_hash or password,
            "role": role,
            "display_name": display_name,
            "email": email,
            "email_greeting": email_greeting,
            "password_changed_at": password_changed_at,
            "force_password_change": force_password_change,
            "permissions": permissions,
        }

    return loaded_users or USERS


USERS = load_users_from_env()
ENV_USERNAMES = set(USERS.keys())
ENV_USERS = {username: user.copy() for username, user in USERS.items()}


def load_persisted_users() -> dict:
    stored = load_json_file(USERS_STORE_PATH, {})
    if not isinstance(stored, dict):
        return {}
    loaded = {}
    for username, item in stored.items():
        if not isinstance(item, dict):
            continue
        user = str(username).strip().lower()
        role = str(item.get("role", "")).strip().lower()
        if role == "ti":
            role = "tecnologia"
        password = str(item.get("password", ""))
        if not user or not password or role not in {"admin", "tecnologia", "invitado"}:
            continue
        loaded[user] = {
            "password": password,
            "role": role,
            "display_name": str(item.get("display_name", user)).strip() or user,
            "email": str(item.get("email", "")).strip().lower(),
            "email_greeting": str(item.get("email_greeting", "")).strip(),
            "password_changed_at": int(item.get("password_changed_at", 0) or 0),
            "force_password_change": bool(item.get("force_password_change", False)),
            "permissions": normalize_permissions(item.get("permissions"), role),
        }
    return loaded


def persist_dynamic_users() -> None:
    try:
        save_json_file(USERS_STORE_PATH, USERS)
    except Exception as exc:
        print(f"[WARN] No se pudo persistir usuarios en {USERS_STORE_PATH}: {exc}", flush=True)


def get_env_user(username: str) -> dict | None:
    return ENV_USERS.get(username)


USERS.update(load_persisted_users())
invite_store = load_json_file(INVITES_STORE_PATH, {})


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


def normalize_header_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize_text(value))


def get_series_by_header_alias(raw: pd.DataFrame, aliases: list[str]) -> pd.Series:
    normalized_map = {normalize_header_key(col): col for col in raw.columns}
    for alias in aliases:
        column_name = normalized_map.get(normalize_header_key(alias))
        if column_name:
            return raw[column_name]
    return pd.Series([""] * len(raw), index=raw.index, dtype="object")


def has_valid_dni(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip() != ""


def has_valid_ip(series: pd.Series) -> pd.Series:
    cleaned = series.fillna("").astype(str).str.strip()
    return (~cleaned.isin(["", "-", "nan", "None", "NULL"])) & cleaned.str.contains(r"[0-9]")


def sign_data(payload: str) -> str:
    return hmac.new(SECRET_KEY.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def create_session_token(username: str) -> str:
    now = int(time.time())
    payload = {
        "u": username,
        "n": secrets.token_hex(8),
        "iat": now,
        "exp": now + SESSION_TIMEOUT_SECONDS,
    }
    payload_json = json.dumps(payload, separators=(",", ":"))
    payload_b64 = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("utf-8")
    signature = sign_data(payload_b64)
    return f"{payload_b64}.{signature}"


def set_session_cookie(response: Response, username: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE,
        value=create_session_token(username),
        httponly=True,
        samesite="lax",
        secure=is_secure_cookie_enabled(),
        path="/",
        max_age=SESSION_TIMEOUT_SECONDS,
    )


def auth_user_payload(username: str, user: dict) -> dict:
    return {
        "username": username,
        "role": user["role"],
        "display_name": user["display_name"],
        "session_timeout_seconds": SESSION_TIMEOUT_SECONDS,
        "password_must_change": password_requires_change({"username": username, **user}),
        "password_max_age_days": int(PASSWORD_MAX_AGE_SECONDS / 86400),
        "permissions": user_permissions(user),
    }


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
    expires_at = payload.get("exp")
    if not isinstance(expires_at, int) or int(time.time()) > expires_at:
        return None

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
    if password_requires_change(user):
        raise HTTPException(status_code=428, detail="Debe cambiar su contrasena para continuar")
    if user["role"] not in allowed_roles:
        raise HTTPException(status_code=403, detail="Sin permisos para esta accion")
    return user


def password_requires_change(user: dict) -> bool:
    if user.get("role") == "admin":
        return bool(user.get("force_password_change", False))
    if bool(user.get("force_password_change", False)):
        return True
    changed_at = int(user.get("password_changed_at", 0) or 0)
    if changed_at <= 0:
        return True
    return now_ts() - changed_at >= PASSWORD_MAX_AGE_SECONDS


def require_password_current(request: Request) -> dict:
    user = get_current_user(request)
    if password_requires_change(user):
        raise HTTPException(status_code=428, detail="Debe cambiar su contrasena para continuar")
    return user


def require_permission(request: Request, permission: str) -> dict:
    user = require_password_current(request)
    if not has_permission(user, permission):
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
            get_series_by_header_alias(raw, ["1 NOMBRE", "1NOMBRE", "1°NOMBRE"]),
            get_series_by_header_alias(raw, ["2 NOMBRE", "2NOMBRE", "2°NOMBRE"]),
            get_series_by_header_alias(raw, ["1 APELLIDO", "1APELLIDO", "1°APELLIDO"]),
            get_series_by_header_alias(raw, ["2 APELLIDO", "2APELLIDO", "2°APELLIDO"]),
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


def load_infra_dataframe_from_excel(file_source) -> pd.DataFrame:
    raw = pd.read_excel(file_source, dtype=str).fillna("")
    raw.columns = [clean_value(col) for col in raw.columns]
    processed = pd.DataFrame()
    processed["ip"] = get_series_by_header_alias(raw, ["IPAddress", "IP Address", "IP"]).map(clean_value).str.strip()
    processed["tipo_vms_ts"] = get_series_by_header_alias(raw, ["VMM/TS", "VMS/TS", "VMM TS"]).map(clean_value)
    processed["hostname_infra"] = get_series_by_header_alias(raw, ["HOSTNAME INFRA", "Hostname Infra"]).map(clean_value)
    processed["sistema_operativo"] = get_series_by_header_alias(raw, ["Sistema Operativo", "SO"]).map(clean_value)
    processed["fecha_entrega"] = format_date(get_series_by_header_alias(raw, ["FECHA ENTREGA VMS", "Fecha Entrega"]))
    processed["ip_norm"] = processed["ip"].map(normalize_text)
    return processed[processed["ip_norm"] != ""].fillna("")


def build_vms_dashboard_data() -> dict:
    inventory = ensure_data_loaded().copy()
    infra = infra_df_global.copy()
    if infra.empty:
        return {
            "archivo": infra_file_name,
            "total_infra": 0,
            "total_asignadas": 0,
            "total_libres": 0,
            "por_area": [],
            "por_centro_costo": [],
            "por_so": [],
            "asignadas": [],
            "libres": [],
        }

    assigned = inventory[has_valid_ip(inventory["ip"]) & has_valid_dni(inventory["dni"])].copy()
    assigned["ip_norm"] = assigned["ip"].map(normalize_text)
    assigned = assigned.drop_duplicates(subset=["ip_norm"], keep="first")

    merged = infra.merge(
        assigned[["ip_norm", "dni", "nombre_completo", "area", "centro_costo", "hostname", "ticket"]],
        on="ip_norm",
        how="left",
    ).fillna("")
    merged["estado_cruce"] = merged["dni"].apply(lambda value: "ASIGNADA" if clean_value(value) else "LIBRE")

    assigned_rows = merged[merged["estado_cruce"] == "ASIGNADA"].copy()
    free_rows = merged[merged["estado_cruce"] == "LIBRE"].copy()

    return {
        "archivo": infra_file_name,
        "total_infra": int(len(infra)),
        "total_asignadas": int(len(assigned_rows)),
        "total_libres": int(len(free_rows)),
        "por_area": summarize_group(assigned_rows.rename(columns={"area": "area"}), "area", limit=12),
        "por_centro_costo": summarize_group(assigned_rows.rename(columns={"centro_costo": "centro_costo"}), "centro_costo", limit=12),
        "por_so": summarize_group(merged.rename(columns={"sistema_operativo": "sistema_operativo"}), "sistema_operativo", limit=8),
        "asignadas": assigned_rows.head(500).to_dict(orient="records"),
        "libres": free_rows.head(500).to_dict(orient="records"),
    }


def load_applications_store() -> pd.DataFrame:
    raw = load_json_file(APPLICATIONS_STORE_PATH, [])
    if not isinstance(raw, list):
        raw = []
    return pd.DataFrame(raw)


def save_applications_store(df: pd.DataFrame) -> None:
    records = df.fillna("").to_dict(orient="records")
    save_json_file(APPLICATIONS_STORE_PATH, records)


def ensure_applications_loaded() -> pd.DataFrame:
    global applications_df_global
    if applications_df_global.empty:
        applications_df_global = load_applications_store()
    return applications_df_global


def normalize_bool(value: object) -> bool:
    return normalize_text(value) in {"1", "true", "si", "yes", "ok", "installed", "instalado"}


def application_row_status(row: dict) -> str:
    carbon_ok = normalize_bool(row.get("carbon_black_installed", ""))
    anyconnect_ok = normalize_bool(row.get("anyconnect_installed", ""))
    return "ALERTA" if not carbon_ok or not anyconnect_ok else "OK"


def smart_search_applications(df: pd.DataFrame, query: str) -> pd.DataFrame:
    normalized_query = normalize_text(query)
    if df.empty or not normalized_query:
        return df
    blob = df.fillna("").astype(str).apply(lambda row: " ".join(normalize_text(value) for value in row), axis=1)
    terms = [term for term in normalized_query.split() if term]
    mask = pd.Series(True, index=df.index)
    for term in terms:
        mask &= blob.str.contains(term, na=False)
    return df[mask].copy()


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


def exact_match_search(df: pd.DataFrame, query: str) -> pd.DataFrame | None:
    cleaned_query = clean_value(query)
    normalized_query = normalize_text(cleaned_query)
    if not normalized_query or "," in cleaned_query:
        return None

    area_matches = df["area"].map(normalize_text) == normalized_query
    if area_matches.any():
        return df[area_matches].copy()

    centro_matches = df["centro_costo"].map(normalize_text) == normalized_query
    if centro_matches.any():
        return df[centro_matches].copy()

    return None


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

    dni_mask = has_valid_dni(subset["dni"])
    ip_mask = has_valid_ip(subset["ip"])

    solicitudes = (
        subset[dni_mask]
        .groupby("ticket")
        .size()
        .reset_index(name="solicitudes")
    )

    activos = (
        subset[dni_mask & ip_mask]
        .groupby("ticket")
        .size()
        .reset_index(name="activos")
    )

    cesados = (
        subset[dni_mask & ~ip_mask]
        .groupby("ticket")
        .size()
        .reset_index(name="cesados")
    )

    modelo_si = (
        subset[dni_mask & ip_mask & (subset["modelo_seguro"] == "SI")]
        .groupby("ticket")
        .size()
        .reset_index(name="modelo_seguro_si")
    )

    modelo_no = (
        subset[dni_mask & ip_mask & (subset["modelo_seguro"] == "NO")]
        .groupby("ticket")
        .size()
        .reset_index(name="personal_nuevo_no")
    )

    summary = solicitudes \
        .merge(activos, on="ticket", how="left") \
        .merge(cesados, on="ticket", how="left") \
        .merge(modelo_si, on="ticket", how="left") \
        .merge(modelo_no, on="ticket", how="left")

    summary = summary.fillna(0)

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
    if not bool(has_valid_ip(pd.Series([row.get("ip", "")])).iloc[0]):
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

    excluded_tags = EXCLUDED_ASSIGNMENT_TAGS + [
        "pruebas",
        "highend",
        "ciberseguridad",
    ]
    if any(tag in combined for tag in excluded_tags):
        return "EXCLUIDO"

    return "ASIGNADO_SERVICIO"


def filter_by_tipo_entorno(df: pd.DataFrame, tipo_entorno: str) -> pd.DataFrame:
    tipo_normalized = normalize_text(tipo_entorno)
    if tipo_normalized not in {"ts", "vms", "vm", "vmm", "anexo"}:
        return df

    normalized_series = df["tipo_entorno"].map(normalize_text)
    normalized_so = df["so"].map(normalize_text)
    if tipo_normalized == "ts":
        mask = normalized_series.str.contains("terminal server", na=False) | normalized_series.eq("ts")
        return df[mask]

    if tipo_normalized == "anexo":
        mask = normalized_series.eq("anexo") | normalized_so.eq("anexo") | df["ip"].map(normalize_text).str.contains("anexo", na=False)
        return df[mask]

    mask = (
        normalized_series.str.contains("vm", na=False)
        | normalized_series.str.contains("vmm", na=False)
        | normalized_series.eq("vms")
        | (
            normalized_series.eq("")
            & ~normalized_so.eq("anexo")
            & ~normalized_series.str.contains("terminal server", na=False)
        )
    )
    return df[mask]


def remote_assignments_only(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    scoped = df.copy()
    scoped = scoped[has_valid_ip(scoped["ip"]) & has_valid_dni(scoped["dni"])]
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


def build_dashboard_snapshot(df: pd.DataFrame) -> dict:
    remote_df = remote_assignments_only(df)
    active_mask = has_valid_dni(df["dni"]) & has_valid_ip(df["ip"])
    cesado_mask = has_valid_dni(df["dni"]) & ~has_valid_ip(df["ip"])

    active_df = df[active_mask].copy()
    if not active_df.empty:
        active_df["clasificacion_asignacion"] = active_df.apply(classify_assignment, axis=1)
    else:
        active_df["clasificacion_asignacion"] = pd.Series(dtype="object")

    ticket_df = df[has_valid_dni(df["dni"]) & (df["ticket"].fillna("").astype(str).str.strip() != "")].copy()

    return {
        "total_registros": int(len(df)),
        "total_activos": int(active_mask.sum()),
        "total_cesados": int(cesado_mask.sum()),
        "tickets_unicos": int(ticket_df["ticket"].nunique()) if not ticket_df.empty else 0,
        "asignados_servicio": int(len(remote_df)),
        "sede_camana": int((active_df["clasificacion_asignacion"] == "SEDE_CAMANA").sum()),
        "sede_chota": int((active_df["clasificacion_asignacion"] == "SEDE_CHOTA").sum()),
        "sede_centro_civico": int((active_df["clasificacion_asignacion"] == "SEDE_CENTRO_CIVICO").sum()),
        "activos_excluidos": int((active_df["clasificacion_asignacion"] == "EXCLUIDO").sum()),
        "por_area": summarize_group(remote_df, "area", limit=20),
        "por_centro_costo": summarize_group(remote_df, "centro_costo", limit=20),
        "por_ticket": ticket_summary(df, limit=20),
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


def build_summary_export(records: list[dict], ordered_columns: list[str]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=ordered_columns)
    df = pd.DataFrame(records)
    return df.reindex(columns=ordered_columns, fill_value="")


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

    dni_mask = has_valid_dni(scoped["dni"])
    ip_mask = has_valid_ip(scoped["ip"])
    ticket_mask = scoped["ticket"].fillna("").astype(str).str.strip() != ""

    scoped["cuenta_solicitud"] = (ticket_mask & dni_mask).astype(int)
    scoped["cuenta_activo"] = (ticket_mask & dni_mask & ip_mask).astype(int)
    scoped["cuenta_cesado"] = (ticket_mask & dni_mask & ~ip_mask).astype(int)
    scoped["modelo_seguro_activo_si"] = (
        (scoped["modelo_seguro"] == "SI") & ticket_mask & dni_mask & ip_mask
    ).astype(int)
    scoped["modelo_seguro_activo_no"] = (
        (scoped["modelo_seguro"] == "NO") & ticket_mask & dni_mask & ip_mask
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
        "solicitudes_ticket_dni": int((ticket_mask & dni_mask).sum()),
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
    if not q:
        return df.copy()

    exact_match = exact_match_search(df, q)
    if exact_match is not None:
        return exact_match

    return smart_search(df, q)


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
        scoped = dashboard_df[has_valid_dni(dashboard_df["dni"]) & has_valid_ip(dashboard_df["ip"])]
        return build_standard_export(scoped)

    if segment == "total_cesados":
        scoped = dashboard_df[has_valid_dni(dashboard_df["dni"]) & ~has_valid_ip(dashboard_df["ip"])]
        return build_standard_export(scoped)

    if segment == "por_ticket":
        base_records = ticket_summary(search_df if q else dashboard_df, limit=500)
        return build_summary_export(
            base_records,
            [
                "ticket",
                "solicitudes",
                "activos",
                "cesados",
                "modelo_seguro_si",
                "personal_nuevo_no",
                "fecha_conexion",
                "fecha_asignacion",
            ],
        )

    if segment == "por_area":
        source_df = search_df if q else dashboard_df
        records = summarize_group(remote_assignments_only(source_df), "area", limit=500)
        return build_summary_export(records, ["area", "cantidad"])

    if segment == "por_centro_costo":
        source_df = search_df if q else dashboard_df
        records = summarize_group(remote_assignments_only(source_df), "centro_costo", limit=500)
        return build_summary_export(records, ["centro_costo", "cantidad"])

    if segment == "search_asignaciones_remotas":
        return build_remote_assignments_export(search_df)

    return pd.DataFrame()


@app.get("/")
def serve_index():
    return FileResponse(BASE_DIR / "index.html")


@app.post("/login")
async def login(request: Request, response: Response):
    body = await request.json()
    username = str(body.get("username", "")).strip().lower()
    password = str(body.get("password", ""))

    user = USERS.get(username)
    env_user = get_env_user(username)
    if (not user or not verify_password(user["password"], password)) and env_user and verify_password(env_user["password"], password):
        merged_user = user.copy() if user else {}
        merged_user.update(env_user)
        if merged_user.get("role") == "admin":
            merged_user["force_password_change"] = False
            merged_user["password_changed_at"] = merged_user.get("password_changed_at", now_ts()) or now_ts()
        USERS[username] = merged_user
        persist_dynamic_users()
        user = merged_user

    if not user or not verify_password(user["password"], password):
        raise HTTPException(status_code=401, detail="Usuario o contrasena incorrecta")

    if not LOGIN_OTP_ENABLED:
        set_session_cookie(response, username)
        return auth_user_payload(username, user)

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
        enviar_correo(
            email,
            codigo,
            user.get("display_name", username),
            user.get("email_greeting", ""),
        )
    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}"
        print(f"[ERROR] No se pudo enviar OTP a {email}: {error_text}", flush=True)
        otp_store.pop(username, None)
        detail = "No se pudo enviar el codigo al correo configurado"
        if SHOW_MAIL_ERROR_DETAILS or user.get("role") == "admin":
            detail = f"{detail}. SMTP: {error_text}"
        raise HTTPException(
            status_code=500,
            detail=detail,
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
    return auth_user_payload(user["username"], user)


@app.get("/password-policy")
def password_policy():
    return PASSWORD_POLICY


@app.get("/entry-access")
def entry_access(token: str = Query(default="")):
    if not ENTRY_ACCESS_TOKEN or not hmac.compare_digest(ENTRY_ACCESS_TOKEN, token):
        raise HTTPException(status_code=404, detail="No encontrado")
    return {"ok": True}


@app.get("/debug-mail")
def debug_mail(
    token: str = Query(default=""),
    email: str = Query(default=""),
):
    if not ENTRY_ACCESS_TOKEN or not hmac.compare_digest(ENTRY_ACCESS_TOKEN, token):
        raise HTTPException(status_code=404, detail="No encontrado")

    destino = clean_value(email).lower() or os.getenv("EMAIL_USER", "").strip()
    if not destino or "@" not in destino:
        raise HTTPException(status_code=400, detail="Indica un correo valido en el parametro email")

    try:
        enviar_correo_html(
            destino,
            "Prueba SMTP | Inventario VMS",
            "Prueba de correo Inventario VMS",
            "Si recibiste este mensaje, el SMTP esta funcionando.",
            "<p>Si recibiste este mensaje, el SMTP de Inventario VMS esta funcionando.</p>",
        )
    except Exception as exc:
        print(f"[ERROR] Prueba SMTP fallida para {destino}: {exc}", flush=True)
        raise HTTPException(status_code=500, detail=f"SMTP fallo: {type(exc).__name__}: {exc}")

    return {"ok": True, "message": f"Correo de prueba enviado a {mask_email(destino)}"}


@app.get("/access-link")
def access_link(token: str = Query(default="")):
    payload = read_action_token(token, "access")
    username = str(payload.get("s", "")).strip().lower() if payload else ""
    if not payload or username not in USERS:
        raise HTTPException(status_code=404, detail="No encontrado")
    return {"ok": True}


@app.post("/admin/invitations")
async def create_invitation(request: Request):
    require_permission(request, "invitaciones")
    body = await request.json()
    email = str(body.get("email", "")).strip().lower()
    display_name = clean_value(body.get("display_name", ""))
    role = str(body.get("role", "invitado")).strip().lower()
    username = str(body.get("username", "")).strip().lower() or email.split("@", 1)[0]
    if role == "ti":
        role = "tecnologia"
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Correo invalido")
    if role not in {"tecnologia", "invitado"}:
        raise HTTPException(status_code=400, detail="Rol invalido para invitacion")

    existing_username, existing_user = find_user_by_email(email)
    if username in USERS:
        existing_username = username
        existing_user = USERS[username]

    if existing_user:
        token = make_action_token("access", existing_username, ttl_seconds=7 * 24 * 60 * 60)
        link = build_public_url(request, token, "access")
        mail_sent = True
        try:
            send_link_email(
                email,
                "Acceso a Inventario VMS",
                "Acceso a la plataforma",
                "Use este enlace para habilitar el ingreso a Inventario VMS en su navegador. Luego ingrese con su usuario y contrasena temporal.",
                link,
            )
        except Exception as exc:
            mail_sent = False
            print(f"[ERROR] No se pudo enviar enlace de acceso a {email}: {type(exc).__name__}: {exc}", flush=True)
        return {
            "ok": True,
            "existing": True,
            "username": existing_username,
            "email": email,
            "role": existing_user.get("role", ""),
            "link": link,
            "mail_sent": mail_sent,
        }

    token = make_action_token("invite", username, ttl_seconds=48 * 60 * 60)
    invite_store[token] = {
        "username": username,
        "email": email,
        "display_name": display_name or username,
        "role": role,
        "created_at": now_ts(),
        "expires_at": now_ts() + 48 * 60 * 60,
        "used": False,
    }
    save_json_file(INVITES_STORE_PATH, invite_store)

    link = build_public_url(request, token, "invite")
    mail_sent = True
    try:
        send_link_email(
            email,
            "Invitacion de acceso | Inventario VMS",
            "Invitacion de acceso",
            "Ha recibido una invitacion para crear su acceso a la plataforma. El enlace vence en 48 horas.",
            link,
        )
    except Exception as exc:
        mail_sent = False
        print(f"[ERROR] No se pudo enviar invitacion a {email}: {type(exc).__name__}: {exc}", flush=True)
    return {"ok": True, "username": username, "email": email, "role": role, "link": link, "mail_sent": mail_sent}


@app.post("/accept-invite")
async def accept_invite(request: Request):
    body = await request.json()
    token = str(body.get("token", "")).strip()
    password = str(body.get("password", ""))
    payload = read_action_token(token, "invite")
    invitation = invite_store.get(token)
    if not payload or not invitation or invitation.get("used") or now_ts() > int(invitation.get("expires_at", 0)):
        raise HTTPException(status_code=400, detail="Invitacion invalida o expirada")

    username = str(invitation["username"]).strip().lower()
    email = str(invitation["email"]).strip().lower()
    display_name = clean_value(invitation.get("display_name", username))
    errors = validate_password_policy(password, username, email, display_name)
    if errors:
        raise HTTPException(status_code=400, detail=" ".join(errors))
    if username in USERS:
        raise HTTPException(status_code=400, detail="El usuario ya existe")

    USERS[username] = {
        "password": hash_password(password),
        "role": invitation["role"],
        "display_name": display_name,
        "email": email,
        "email_greeting": display_name,
        "password_changed_at": now_ts(),
        "force_password_change": False,
        "permissions": ROLE_DEFAULT_PERMISSIONS.get(invitation["role"], ["inventario"]),
    }
    invitation["used"] = True
    invite_store[token] = invitation
    persist_dynamic_users()
    save_json_file(INVITES_STORE_PATH, invite_store)
    return {"ok": True, "username": username, "message": "Cuenta creada. Ya puede iniciar sesion."}


@app.post("/password-reset/request")
async def request_password_reset(request: Request):
    body = await request.json()
    identifier = str(body.get("identifier", "")).strip().lower()
    user_item = None
    username = ""
    for candidate, user in USERS.items():
        if candidate == identifier or str(user.get("email", "")).strip().lower() == identifier:
            username = candidate
            user_item = user
            break
    if user_item and clean_value(user_item.get("email", "")):
        token = make_action_token("reset", username, ttl_seconds=60 * 60)
        password_reset_store[token] = {"username": username, "expires_at": now_ts() + 60 * 60, "used": False}
        link = build_public_url(request, token, "reset")
        send_link_email(
            user_item["email"],
            "Restablecer contrasena | Inventario VMS",
            "Restablecer contrasena",
            "Use este enlace para definir una nueva contrasena. El enlace vence en 60 minutos.",
            link,
        )
    return {"ok": True, "message": "Si el usuario existe, se enviara un enlace al correo registrado."}


@app.post("/password-reset/confirm")
async def confirm_password_reset(request: Request):
    body = await request.json()
    token = str(body.get("token", "")).strip()
    password = str(body.get("password", ""))
    payload = read_action_token(token, "reset")
    reset_item = password_reset_store.get(token)
    if not payload or not reset_item or reset_item.get("used") or now_ts() > int(reset_item.get("expires_at", 0)):
        raise HTTPException(status_code=400, detail="Enlace invalido o expirado")
    username = str(reset_item.get("username", "")).strip().lower()
    user = USERS.get(username)
    if not user:
        raise HTTPException(status_code=400, detail="Usuario no encontrado")
    errors = validate_password_policy(password, username, user.get("email", ""), user.get("display_name", ""))
    if errors:
        raise HTTPException(status_code=400, detail=" ".join(errors))
    user["password"] = hash_password(password)
    user["password_changed_at"] = now_ts()
    user["force_password_change"] = False
    USERS[username] = user
    reset_item["used"] = True
    password_reset_store[token] = reset_item
    persist_dynamic_users()
    return {"ok": True, "message": "Contrasena actualizada."}


@app.post("/change-password")
async def change_password(request: Request):
    user = get_current_user(request)
    body = await request.json()
    current_password = str(body.get("current_password", ""))
    new_password = str(body.get("new_password", ""))

    stored_user = USERS.get(user["username"])
    if not stored_user or not verify_password(stored_user["password"], current_password):
        raise HTTPException(status_code=400, detail="La contrasena actual no es correcta")

    errors = validate_password_policy(
        new_password,
        user["username"],
        stored_user.get("email", ""),
        stored_user.get("display_name", ""),
    )
    if errors:
        raise HTTPException(status_code=400, detail=" ".join(errors))

    stored_user["password"] = hash_password(new_password)
    stored_user["password_changed_at"] = now_ts()
    stored_user["force_password_change"] = False
    USERS[user["username"]] = stored_user
    persist_dynamic_users()
    return {"ok": True, "message": "Contrasena actualizada correctamente."}


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
    set_session_cookie(response, username)

    user = USERS[username]
    return auth_user_payload(username, user)

@app.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...)):
    require_permission(request, "cargar_excel")
    global df_global, current_file_name
    df_global = load_dataframe_from_excel(file.file)
    current_file_name = file.filename or "archivo_subido.xlsx"
    return {
        "mensaje": "Archivo cargado correctamente",
        "registros": int(len(df_global)),
        "archivo": current_file_name,
    }


@app.post("/upload-infra-vms")
async def upload_infra_vms(request: Request, file: UploadFile = File(...)):
    require_permission(request, "dashboard_vms")
    global infra_df_global, infra_file_name
    infra_df_global = load_infra_dataframe_from_excel(file.file)
    infra_file_name = file.filename or "infra_vms.xlsx"
    return {
        "mensaje": "Base de infraestructura cargada correctamente",
        "registros": int(len(infra_df_global)),
        "archivo": infra_file_name,
    }


@app.get("/dashboard-vms")
def dashboard_vms(request: Request):
    require_permission(request, "dashboard_vms")
    return build_vms_dashboard_data()


@app.post("/applications/report")
async def receive_application_report(
    request: Request,
    x_agent_token: str = Header(default=""),
    authorization: str = Header(default=""),
):
    expected = AGENT_REPORT_TOKEN
    received = x_agent_token or authorization.replace("Bearer ", "", 1).strip()
    if expected and not hmac.compare_digest(expected, received):
        raise HTTPException(status_code=403, detail="Token de agente invalido")

    body = await request.json()
    record = {key: clean_value(value) for key, value in body.items()}
    record["reported_at"] = clean_value(record.get("reported_at")) or time.strftime("%Y-%m-%d %H:%M:%S")
    record["dni"] = clean_value(record.get("dni"))
    record["nombre_completo"] = clean_value(record.get("nombre_completo"))
    record["hostname"] = clean_value(record.get("hostname")) or clean_value(record.get("computer_name"))
    record["estado_alerta"] = application_row_status(record)

    if not record["dni"]:
        raise HTTPException(status_code=400, detail="DNI requerido")

    global applications_df_global
    applications_df_global = ensure_applications_loaded()
    new_df = pd.DataFrame([record])
    if applications_df_global.empty:
        applications_df_global = new_df
    else:
        if "dni" not in applications_df_global.columns:
            applications_df_global["dni"] = ""
        if "hostname" not in applications_df_global.columns:
            applications_df_global["hostname"] = ""
        key_mask = (
            (applications_df_global["dni"].fillna("").astype(str) == record["dni"])
            & (applications_df_global["hostname"].fillna("").astype(str).str.lower() == record["hostname"].lower())
        )
        applications_df_global = applications_df_global[~key_mask]
        applications_df_global = pd.concat([applications_df_global, new_df], ignore_index=True)
    save_applications_store(applications_df_global)
    return {"ok": True, "estado_alerta": record["estado_alerta"]}


@app.get("/applications-tto")
def applications_tto(
    request: Request,
    q: str = Query(default=""),
    limit: int = Query(default=500, ge=1, le=5000),
):
    require_permission(request, "aplicaciones_tto")
    df = smart_search_applications(ensure_applications_loaded(), q).head(limit)
    if df.empty:
        return []
    return df.fillna("").to_dict(orient="records")


@app.get("/vms")
def get_vms(
    request: Request,
    q: str = Query(default=""),
    tipo_entorno: str = Query(default="todos"),
    status: str = Query(default="todos"),
    limit: int = Query(default=200, ge=1, le=5000),
):
    require_permission(request, "inventario")
    result = get_search_scoped_df(q, tipo_entorno, status)
    result = result.drop(columns=["search_blob"], errors="ignore").head(limit)
    return result.fillna("").to_dict(orient="records")


@app.get("/dashboard")
def dashboard(
    request: Request,
    status: str = Query(default="todos"),
    tipo_entorno: str = Query(default="todos"),
):
    require_permission(request, "inventario")
    df = get_dashboard_scoped_df(tipo_entorno, status)
    snapshot = build_dashboard_snapshot(df)
    snapshot["archivo"] = current_file_name
    return snapshot


@app.get("/search-dashboard")
def search_dashboard(
    request: Request,
    q: str = Query(default=""),
    tipo_entorno: str = Query(default="todos"),
    status: str = Query(default="todos"),
):
    require_permission(request, "inventario")
    result = get_search_scoped_df(q, tipo_entorno, status)
    data = build_search_dashboard(result)
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
    require_permission(request, "inventario")
    result = get_search_scoped_df(q, tipo_entorno, status)
    return build_assignment_pivot(result, limit=limit)


@app.get("/export-search-assignments")
def export_search_assignments(
    request: Request,
    q: str = Query(default=""),
    tipo_entorno: str = Query(default="todos"),
    status: str = Query(default="todos"),
):
    require_permission(request, "exportar")
    result = get_search_scoped_df(q, tipo_entorno, status)
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
    require_permission(request, "exportar")
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
    require_permission(request, "inventario")
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
