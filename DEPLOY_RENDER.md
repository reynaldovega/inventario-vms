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
- `SMTP_SECURITY=ssl`
- `COOKIE_SECURE=true`
- `SESSION_TIMEOUT_SECONDS=7200`
- `PASSWORD_MAX_AGE_SECONDS=7776000`
- `DATA_DIR=/var/data`
- `PUBLIC_BASE_URL=https://inventario-vms.onrender.com`
- `AGENT_REPORT_TOKEN=una-clave-larga-privada`
- `ENTRY_ACCESS_TOKEN=otra-clave-larga-privada`

### Persistencia de contrasenas cambiadas

La app permite que el administrador coloque una contrasena inicial en `APP_USERS_JSON`.
Para usuarios con rol `tecnologia` o `invitado`, esa contrasena inicial es temporal:

1. El usuario entra con la contrasena inicial.
2. Recibe el codigo de verificacion por correo.
3. La web le exige cambiar la contrasena antes de usar la plataforma.
4. La nueva contrasena queda guardada en `DATA_DIR/users.json`.
5. A los 90 dias vuelve a pedir cambio de contrasena.

Para que esas contrasenas nuevas no se pierdan al redeploy, Render debe tener un disco persistente montado en `/var/data`.
El `render.yaml` de este proyecto ya incluye:

```yaml
envVars:
  - key: DATA_DIR
    value: /var/data
disks:
  - name: inventario-vms-data
    mountPath: /var/data
    sizeGB: 1
```

Si configuras el servicio manualmente desde la pantalla de Render, crea un `Disk` con mount path `/var/data` y agrega la variable `DATA_DIR=/var/data`.

### Entrada privada sin login publico

La pantalla de login no se muestra si la persona abre la web directamente.
Para habilitar el login en un navegador autorizado, usa una entrada privada:

```text
https://inventario-vms.onrender.com/?entry=TU_ENTRY_ACCESS_TOKEN
```

Ese token debe ser igual al valor de `ENTRY_ACCESS_TOKEN` en Render.
Luego de abrir ese enlace una vez, ese navegador queda autorizado para ver el login.
Los usuarios invitados tambien quedan autorizados despues de aceptar su invitacion.

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

## 4.1 Permisos por usuario

Puedes controlar que pestañas ve cada usuario agregando `permissions`.
Si no agregas `permissions`, la app usa los permisos por defecto del rol.

Permisos disponibles:

- `inventario`: pestaña Inventario VMS.
- `dashboard_vms`: pestaña Dashboard y carga/cruce de base VMS infraestructura.
- `aplicaciones_tto`: pestaña Aplicaciones TTO.
- `invitaciones`: pestaña Invitaciones.
- `cargar_excel`: permite subir Excel en Inventario VMS.
- `exportar`: permite exportar reportes.

Ejemplo: invitado que solo puede ver Inventario:

```json
{
  "username": "jrojas",
  "password": "123456",
  "role": "invitado",
  "display_name": "Juan Rojas",
  "email": "juan.acevedo11.03@gmail.com",
  "email_greeting": "Hola Juan",
  "permissions": ["inventario"]
}
```

Ejemplo: tecnologia con Inventario y Dashboard, pero sin Aplicaciones TTO:

```json
{
  "username": "miriam.gamboa",
  "password": "654321",
  "role": "tecnologia",
  "display_name": "Miriam Gamboa",
  "email": "mgamboa5797@gmail.com",
  "email_greeting": "Holi amix Miriam",
  "permissions": ["inventario", "dashboard_vms", "cargar_excel"]
}
```

Ejemplo: usuario con solo Aplicaciones TTO:

```json
"permissions": ["aplicaciones_tto"]
```

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

## 5.1.1 Recomendacion SMTP

Para Gmail:

```env
EMAIL_USER=tu_correo@gmail.com
EMAIL_PASS=contrasena_de_aplicacion_gmail
SMTP_HOST=smtp.gmail.com
SMTP_PORT=465
SMTP_SECURITY=ssl
SMTP_TIMEOUT_SECONDS=20
```

`EMAIL_PASS` debe ser una contrasena de aplicacion de Google, no la contrasena normal del correo.

Para Brevo/Sendinblue, recomendado si Gmail bloquea o demora:

```env
EMAIL_USER=tu_login_smtp_de_brevo
EMAIL_PASS=tu_clave_smtp_de_brevo
SMTP_HOST=smtp-relay.brevo.com
SMTP_PORT=587
SMTP_SECURITY=starttls
SMTP_TIMEOUT_SECONDS=20
```

Puedes probar el correo sin iniciar sesion con:

```text
https://inventario-vms.onrender.com/debug-mail?token=TU_ENTRY_ACCESS_TOKEN&email=correo@dominio.com
```

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
