import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os
from openpyxl import load_workbook
from openpyxl.styles import Font
import glob
import sys
import subprocess

# ==============================
# 1️⃣ Conexión a PostgreSQL
# ==============================
usuario = 'postgres'
contraseña = 'pasante'
host = 'localhost'
puerto = '5432'
base_datos = 'prepago'
connection_string = f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}'
engine = create_engine(connection_string)

# ==============================
# Función para quitar negrita de encabezados en Excel
# ==============================
def quitar_negrita_excel(ruta_archivo):
    wb = load_workbook(ruta_archivo)
    for ws in wb.worksheets:
        for cell in ws[1]:
            cell.font = Font(bold=False)
    wb.save(ruta_archivo)

# ==============================
# 2️⃣ Detectar último Excel en carpeta prepago
# ==============================
carpeta_base = r'C:\Users\pasante.ti2\Desktop\bases prepago'
archivos_excel = [
    f for f in glob.glob(os.path.join(carpeta_base, "*.xlsx"))
    if "_con_anio_mes" not in f and "_incompletos" not in f
]

if not archivos_excel:
    raise FileNotFoundError("❌ No se encontró ningún archivo Excel en la carpeta.")

ruta_base = max(archivos_excel, key=os.path.getmtime)
print(f"📥 Procesando archivo: {ruta_base}")

# ==============================
# 3️⃣ Leer Excel
# ==============================
df = pd.read_excel(ruta_base, sheet_name=0)
df.columns = [c.lower().strip() for c in df.columns]
print(f"✅ Total de registros cargados: {len(df)}")

# ==============================
# 4️⃣ Año, mes y texto_extraido actuales
# ==============================
fecha_actual = datetime.today()
meses = {
    1: "ENERO", 2: "FEBRERO", 3: "MARZO", 4: "ABRIL",
    5: "MAYO", 6: "JUNIO", 7: "JULIO", 8: "AGOSTO",
    9: "SEPTIEMBRE", 10: "OCTUBRE", 11: "NOVIEMBRE", 12: "DICIEMBRE"
}
mes_actual = meses[fecha_actual.month]

# ==============================
# 5️⃣ Normalización y validaciones con regla "NO REGISTRA"
# ==============================

# Normalización básica
df['nombre_completo'] = df.get('nombre_completo', '').fillna('').astype(str)
df['identificacion'] = df.get('identificacion', '').fillna('').astype(str)
df['celular'] = df.get('celular', '').fillna('').astype(str)

# 1️⃣ Rellenar con "NO REGISTRA" si nombre vacío pero identificación existe
mask_nombre_vacio_id_presente = (
    (df['nombre_completo'].str.strip() == '') & 
    (df['identificacion'].str.strip() != '')
)
df.loc[mask_nombre_vacio_id_presente, 'nombre_completo'] = 'NO REGISTRA'

# Función para normalizar celular
def normalizar_celular(c):
    numeros = ''.join(filter(str.isdigit, str(c)))  # Quitar caracteres no numéricos
    if len(numeros) == 9:
        return '0' + numeros
    elif len(numeros) == 8:
        return '09' + numeros
    else:
        return numeros  # Para detectar incompletos

# Aplicar normalización
df['celular_norm'] = df['celular'].apply(normalizar_celular)

# Reglas de registros incompletos
mask_incompleto_id_vacia = (
    (df['identificacion'].str.strip() == '') & 
    (df['nombre_completo'].str.strip() != '')
)
mask_celular_invalido = df['celular_norm'].apply(lambda x: len(x) < 10)
mask_incompletos = mask_incompleto_id_vacia | mask_celular_invalido

# Duplicados de celular
duplicados_cel = df[
    df.duplicated('celular_norm', keep=False) & 
    (df['celular_norm'] != '')
].copy()

# ==============================
# 6️⃣ Si hay errores, crear archivo INCORRECTA y salir
# ==============================
if mask_incompletos.any() or not duplicados_cel.empty:
    nombre_archivo = f"INCORRECTA_{mes_actual}"
    ruta_incompletos = os.path.join(carpeta_base, f'{nombre_archivo}.xlsx')

    with pd.ExcelWriter(ruta_incompletos, engine='openpyxl') as writer:
        if mask_incompletos.any():
            incompletos = df.loc[mask_incompletos].copy()
            incompletos.to_excel(writer, sheet_name='Incompletos', index=False)
            print(f"❌ {len(incompletos)} registros incompletos detectados.")
        if not duplicados_cel.empty:
            duplicados_cel.to_excel(writer, sheet_name='Duplicados_Celular', index=False)
            print(f"❌ {len(duplicados_cel)} registros con celular duplicado.")

    quitar_negrita_excel(ruta_incompletos)
    sys.exit("🚫 Proceso detenido por registros incompletos o duplicados.")

# Para registros correctos, reemplazamos el celular por el normalizado
df['celular'] = df['celular_norm']
df.drop(columns=['celular_norm'], inplace=True)

# ==============================
# 7️⃣ Añadir año, mes y texto_extraido
# ==============================
df['año'] = fecha_actual.year
df['mes'] = mes_actual
df['texto_extraido'] = fecha_actual.strftime("%d%b%Y").lower()

# Reordenar columnas
cols = ['año', 'mes', 'texto_extraido'] + [c for c in df.columns if c not in ['año', 'mes', 'texto_extraido']]
df = df[cols]

# ==============================
# Evitar duplicados por id_cliente
# ==============================
if 'id_cliente' in df.columns:
    df = df.drop_duplicates(subset=['id_cliente'])
    print(f"✅ Total registros únicos por cliente: {len(df)}")

# ==============================
# 8️⃣ Guardar archivo CORRECTA
# ==============================
nombre_archivo = f"CORRECTA_{mes_actual}"
ruta_copia = os.path.join(carpeta_base, f'{nombre_archivo}.xlsx')
df.to_excel(ruta_copia, index=False)
quitar_negrita_excel(ruta_copia)
print(f"📂 Base correcta guardada en: {ruta_copia}")
print(f"✅ Proceso finalizado: {len(df)} registros válidos.")

# ==============================
# 9️⃣ Ejecutar cargarpre.py si CORRECTA existe
# ==============================
if os.path.exists(ruta_copia):
    ruta_cargarpre = r"C:\Users\pasante.ti2\Desktop\cargarBases-20250917T075622Z-1-001\cargarBases\cargarpre.py"
    print(f"🚀 Ejecutando cargarpre.py...")
    subprocess.run(["python", ruta_cargarpre])
else:
    print("❌ No se encontró archivo CORRECTA. No se ejecuta cargarpre.py.")
