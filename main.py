from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI()

# 🔥 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

df_global = None


# 🔧 PROCESAR DATA (FIX DEFINITIVO)
def procesar_df(df):
    try:
        df = df.fillna("")

        print("📊 COLUMNAS:", list(df.columns))

        # 🔥 detectar columna IP
        col_ip = None
        for col in df.columns:
            if "ip" in col.lower() or "ts" in col.lower():
                col_ip = col
                break

        # 🔥 FIX CLAVE: fallback a columna 0
        df["ip"] = df[col_ip] if col_ip else df.iloc[:, 0]

        # 🔒 columnas seguras
        def safe_col(index):
            return df.iloc[:, index] if index < len(df.columns) else ""

        df["so"] = safe_col(1)
        df["dni"] = safe_col(5)
        df["area"] = safe_col(3)
        df["centro_costo"] = safe_col(4)
        df["hostname"] = safe_col(16)
        df["ticket"] = safe_col(23)
        df["fecha_asignacion"] = safe_col(25)

        # 📅 fecha
        df["fecha_asignacion"] = pd.to_datetime(
            df["fecha_asignacion"], errors="coerce"
        ).dt.strftime("%d/%m/%Y")

        # 👤 nombre
        df["nombre_completo"] = (
            df.get("1°NOMBRE", "").astype(str) + " " +
            df.get("2°NOMBRE", "").astype(str) + " " +
            df.get("1°APELLIDO", "").astype(str) + " " +
            df.get("2°APELLIDO", "").astype(str)
        ).str.strip()

        # 🔥 limpiar IP vacía (AHORA SÍ FUNCIONA)
        df = df[df["ip"].astype(str).str.strip() != ""]

        df["estado"] = "ACTIVO"

        print("✅ FILAS PROCESADAS:", len(df))

        return df

    except Exception as e:
        print("🔥 ERROR procesar_df:", e)
        return pd.DataFrame()


# 📥 SUBIR EXCEL
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    global df_global

    try:
        df = pd.read_excel(file.file)
        df_global = procesar_df(df)

        print("📥 DATA CARGADA:", len(df_global))

        return {"mensaje": "Archivo cargado correctamente"}

    except Exception as e:
        print("🔥 ERROR UPLOAD:", e)
        return {"mensaje": "Error al procesar archivo"}


# 📊 CARGAR DATA
def cargar_data():
    global df_global

    if df_global is not None:
        return df_global

    return pd.DataFrame()  # 👈 vacío si no hay upload


# 📡 ENDPOINTS
@app.get("/vms")
def get_vms():
    try:
        df = cargar_data()
        print("📊 VMS:", len(df))
        return df.to_dict(orient="records")

    except Exception as e:
        print("🔥 ERROR /vms:", e)
        return []


@app.get("/dashboard")
def dashboard():
    try:
        df = cargar_data()

        if df.empty:
            return {
                "total_activos": 0,
                "por_area": []
            }

        activos = df[df["estado"] == "ACTIVO"]
        por_area = df.groupby("area").size().reset_index(name="cantidad")

        return {
            "total_activos": len(activos),
            "por_area": por_area.to_dict(orient="records")
        }

    except Exception as e:
        print("🔥 ERROR /dashboard:", e)
        return {"total_activos": 0, "por_area": []}