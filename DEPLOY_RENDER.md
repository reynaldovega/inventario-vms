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
    "display_name": "Administrador"
  },
  {
    "username": "miriam.gamboa",
    "password": "123456",
    "role": "ti",
    "display_name": "Miriam Gamboa"
  },
  {
    "username": "invitado",
    "password": "lectura2026",
    "role": "invitado",
    "display_name": "Invitado"
  }
]
```

## 4. Como agregar un usuario nuevo

Solo agrega otro bloque dentro del JSON:

```json
{
  "username": "juan.perez",
  "password": "clave123",
  "role": "ti",
  "display_name": "Juan Perez"
}
```

Roles permitidos:

- `admin`
- `ti`
- `invitado`

## 5. Que hace cada rol

- `admin`: acceso total y puede subir archivo.
- `ti`: puede usar la app y subir archivo.
- `invitado`: solo lectura.

## 6. Importante sobre el Excel

La app arranca leyendo el archivo `.xlsx` que esta dentro del proyecto.

Si en produccion subes otro Excel desde la web:

- funcionara mientras el servicio siga vivo
- si Render reinicia el servicio, esa carga se pierde

Si luego quieres persistencia real, conviene guardar el archivo en disco persistente o en una base de datos.
