# Deploy En Render

## 1. Subir el proyecto a GitHub

Sube estos archivos al repositorio:

- `main.py`
- `index.html`
- `requirements.txt`
- `render.yaml`
- `.gitignore`
- tu archivo `.xlsx`

## 2. Crear el servicio en Render

1. Entra a Render.
2. Crea un nuevo `Web Service`.
3. Conecta tu repositorio.
4. Render debe detectar `render.yaml` automaticamente.

## 3. Variables de entorno recomendadas

En Render, en `Environment`, agrega:

### `APP_SECRET_KEY`

Usa una clave larga y privada. Ejemplo:

```text
inventario-vms-2026-clave-super-segura
```

### `APP_USERS_JSON`

Aqui defines los usuarios del sistema en formato JSON.

Ejemplo:

```json
[
  {
    "username": "admin",
    "password": "Sayayin*rey25*",
    "role": "admin",
    "display_name": "Administrador",
    "email": "admin@tuempresa.com",
    "email_greeting": "Hola Rey"
  },
  {
    "username": "miriam.gamboa",
    "password": "123456",
    "role": "tecnologia",
    "display_name": "Miriam Gamboa",
    "email": "miriam.gamboa@tuempresa.com",
    "email_greeting": "Holi amix Miriam"
  },
  {
    "username": "invitado",
    "password": "lectura2026",
    "role": "invitado",
    "display_name": "Invitado",
    "email": "invitado@tuempresa.com",
    "email_greeting": "Hola Invitado"
  }
]
```

### `EMAIL_USER` y `EMAIL_PASS`

Credenciales del correo que enviara el codigo de verificacion.

Si usas Gmail:

- `EMAIL_USER`: tu cuenta remitente
- `EMAIL_PASS`: la contrasena de aplicacion

Opcionalmente tambien puedes definir:

- `SMTP_HOST`
- `SMTP_PORT`
- `COOKIE_SECURE=true`
- `SESSION_TIMEOUT_SECONDS=7200`

## 4. Como agregar un usuario nuevo

Solo agrega otro bloque dentro del JSON:

```json
  {
    "username": "juan.perez",
    "password": "clave123",
    "role": "tecnologia",
    "display_name": "Juan Perez",
    "email": "juan.perez@tuempresa.com",
    "email_greeting": "Hola Juan"
  }
```

`email_greeting` es opcional. Si lo defines, ese texto se usa como saludo personalizado en el correo. Si no lo defines, la app usara `display_name`.

Roles permitidos:

- `admin`
- `tecnologia`
- `invitado`

## 5. Que hace cada rol

- `admin`: acceso total y puede subir archivo.
- `tecnologia`: puede usar la app y subir archivo.
- `invitado`: solo lectura.

## 5.1 Doble autenticacion por correo

El login ahora funciona en 2 pasos:

1. el usuario ingresa `usuario + contrasena`
2. la app envia un codigo de 6 digitos al correo configurado
3. el usuario escribe ese codigo y recien alli se crea la sesion

Si un usuario no tiene `email`, no podra terminar el ingreso.

## 5.2 Tiempo de sesion

Puedes controlar el tiempo maximo de sesion e inactividad con una sola variable:

- `SESSION_TIMEOUT_SECONDS`

Ejemplos:

- `7200` = 2 horas
- `3600` = 1 hora
- `1800` = 30 minutos

La app usa ese valor tanto para la cookie del backend como para el cierre por inactividad en el frontend.

## 6. Importante sobre el Excel

La app arranca leyendo el archivo `.xlsx` que esta dentro del proyecto.

Si en produccion subes otro Excel desde la web:

- funcionara mientras el servicio siga vivo
- si Render reinicia el servicio, esa carga se pierde

Si luego quieres persistencia real, conviene guardar el archivo en disco persistente o en una base de datos.
