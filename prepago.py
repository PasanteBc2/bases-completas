import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font
import glob
import os
import sys
import unicodedata
import re
import logging
import cargarpre  # Script de carga

# ==============================
# 1️⃣ Conexión segura a PostgreSQL
# ==============================
usuario = 'postgres'
contraseña = 'pasante'
host = 'localhost'
puerto = '5432'
base_datos = 'prepago'

connection_string = f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}'

try:
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        print("✅ Conexión a PostgreSQL establecida correctamente.")
except OperationalError as e:
    sys.exit(f"No se pudo conectar a la base de datos: {e}")
except Exception as e:
    sys.exit(f"Error inesperado al conectar a la base de datos: {e}")


## ==============================
# Función para quitar negrita en Excel
# ==============================
def quitar_negrita_excel(ruta_archivo):
    from openpyxl import load_workbook
    from openpyxl.styles import Font
    try:
        wb = load_workbook(ruta_archivo)
        for ws in wb.worksheets:
            for cell in ws[1]:
                cell.font = Font(bold=False)
        wb.save(ruta_archivo)
    except Exception:
        print(f"Error quitando negrita en {ruta_archivo}")



# ==============================
# 2️⃣ Detectar último Excel en carpeta
# ==============================
carpeta_base = r'C:\Users\pasante.ti2\Desktop\bases prepago'

try:
    archivos_excel = [
        f for f in glob.glob(os.path.join(carpeta_base, "*.xlsx"))
        if "_con_anio_mes" not in f and "_incompletos" not in f
    ]
    if not archivos_excel:
        raise FileNotFoundError("No se encontró ningún archivo Excel en la carpeta.")
    ruta_base = max(archivos_excel, key=os.path.getmtime)
    print(f"Procesando archivo: {ruta_base}")
except Exception as e:
    sys.exit(f"Error detectando archivos: {e}")

# -----------------------------
# 3️⃣ Leer archivo
# -----------------------------
try:
    if ruta_base.lower().endswith(".csv"):
        df = pd.read_csv(ruta_base)
    else:
        df = pd.read_excel(ruta_base, sheet_name=0)
except Exception as e:
    logging.exception(f"❌ Error leyendo el archivo {ruta_base}: {e}")
    raise SystemExit(e)

df.columns = [c.lower().strip() for c in df.columns]
logging.info(f"✅ Total de registros cargados: {len(df)}")

# -----------------------------
# 4️⃣ Limpiar desc_forma_pago
# -----------------------------
def limpiar_sin_tildes(texto):
    if pd.isna(texto):
        return ""
    try:
        texto = texto.encode('latin1', errors='ignore').decode('utf-8', errors='ignore')
    except Exception:
        pass
    texto = unicodedata.normalize('NFKD', texto)
    texto = texto.encode('ascii', 'ignore').decode('utf-8')
    texto = texto.replace('/', ' ')
    texto = re.sub(r'[^A-Za-z0-9Ññ\s.,-]', '', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    texto = texto.upper()
    return texto

if 'desc_forma_pago' in df.columns:
    df['desc_forma_pago'] = df['desc_forma_pago'].astype(str).apply(limpiar_sin_tildes)
    logging.info("🧹 Columna 'desc_forma_pago' limpiada.")

# -----------------------------
# 5️⃣ Validaciones y normalizaciones (igual que tu prepago original)
# -----------------------------
for col_exp in ['nombre_completo', 'identificacion', 'celular']:
    if col_exp not in df.columns:
        logging.warning(f"⚠️ Columna esperada '{col_exp}' no encontrada. Se creará vacía.")
        df[col_exp] = ""

df['nombre_completo'] = df.get('nombre_completo', '').fillna('').astype(str)
df['identificacion'] = df.get('identificacion', '').fillna('').astype(str)
df['celular'] = df.get('celular', '').fillna('').astype(str)

mask_incompleto_id_vacia = (df['identificacion'].str.strip() == '') & (df['nombre_completo'].str.strip() != '')
mask_celular_invalido = df['celular'].apply(lambda x: len(''.join(filter(str.isdigit, x))) < 8)
mask_incompletos = mask_incompleto_id_vacia | mask_celular_invalido

df['celular_norm'] = df['celular'].apply(lambda x: ''.join(filter(str.isdigit, x)))
duplicados_cel = df[df.duplicated('celular_norm', keep=False) & (df['celular_norm'] != '')].copy()

if mask_incompletos.any() or not duplicados_cel.empty:
    nombre_archivo = f"INCORRECTA_{datetime.today().month}.xlsx"
    ruta_incompletos = os.path.join(carpeta_base, nombre_archivo)
    with pd.ExcelWriter(ruta_incompletos, engine='openpyxl') as writer:
        if mask_incompletos.any():
            df.loc[mask_incompletos].to_excel(writer, sheet_name='Incompletos', index=False)
        if not duplicados_cel.empty:
            duplicados_cel.to_excel(writer, sheet_name='Duplicados_Celular', index=False)
    quitar_negrita_excel(ruta_incompletos)
    logging.error("🚫 Proceso detenido: registros incorrectos.")
    sys.exit("Proceso detenido.")

df.drop(columns=['celular_norm'], inplace=True)

# ==============================
# 8️⃣ Guardar CORRECTA
# ==============================
ruta_correcta = os.path.join(carpeta_base, "CORRECTA.xlsx")
df.to_excel(ruta_correcta, index=False)
print(f"✅ Archivo CORRECTA guardado: {ruta_correcta}")

# ==============================
# 9️⃣ Ejecutar cargarpre.py con la MISMA conexión
# ==============================
try:
    cargarpre.run_cargarpre(engine, ruta_correcta)  # 👈 usa engine del primer script
    print("✅ cargarpre.py ejecutado correctamente usando la misma conexión.")
except Exception as e:
    print(f"❌ Error ejecutando cargarpre.py: {e}")