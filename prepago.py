import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font
import glob
import os
import sys
import subprocess

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

# ==============================
# Función para quitar negrita en Excel
# ==============================
def quitar_negrita_excel(ruta_archivo):
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

# ==============================
# 3️⃣ Leer Excel
# ==============================
try:
    df = pd.read_excel(ruta_base)
    df.columns = [c.lower().strip() for c in df.columns]
    print(f"✅ Total registros cargados: {len(df)}")
except Exception as e:
    sys.exit(f"Error leyendo Excel: {e}")

# ==============================
# 4️⃣ Año, mes y texto extraído
# ==============================
fecha_actual = datetime.today()
meses = {1: "ENERO",2: "FEBRERO",3: "MARZO",4: "ABRIL",5: "MAYO",6: "JUNIO",
         7: "JULIO",8: "AGOSTO",9: "SEPTIEMBRE",10: "OCTUBRE",11: "NOVIEMBRE",12: "DICIEMBRE"}
mes_actual = meses[fecha_actual.month]

# ==============================
# 5️⃣ Normalización y validaciones
# ==============================
df['nombre_completo'] = df.get('nombre_completo', '').fillna('').astype(str)
df['identificacion'] = df.get('identificacion', '').fillna('').astype(str)
df['celular'] = df.get('celular', '').fillna('').astype(str)

mask_nombre_vacio_id_presente = (df['nombre_completo'].str.strip() == '') & (df['identificacion'].str.strip() != '')
df.loc[mask_nombre_vacio_id_presente, 'nombre_completo'] = 'NO REGISTRA'

def normalizar_celular(c):
    numeros = ''.join(filter(str.isdigit, str(c)))
    if len(numeros) == 9:
        return '0' + numeros
    elif len(numeros) == 8:
        return '09' + numeros
    return numeros

df['celular_norm'] = df['celular'].apply(normalizar_celular)

mask_incompleto_id_vacia = (df['identificacion'].str.strip() == '') & (df['nombre_completo'].str.strip() != '')
mask_celular_invalido = df['celular_norm'].apply(lambda x: len(x) < 10)
mask_incompletos = mask_incompleto_id_vacia | mask_celular_invalido

duplicados_cel = df[df.duplicated('celular_norm', keep=False) & (df['celular_norm'] != '')].copy()

# ==============================
# 6️⃣ Guardar INCORRECTA si hay errores
# ==============================
if mask_incompletos.any() or not duplicados_cel.empty:
    ruta_incorrecta = os.path.join(carpeta_base, f"INCORRECTA_{mes_actual}.xlsx")
    try:
        with pd.ExcelWriter(ruta_incorrecta, engine='openpyxl') as writer:
            if mask_incompletos.any():
                df.loc[mask_incompletos].to_excel(writer, sheet_name='Incompletos', index=False)
            if not duplicados_cel.empty:
                duplicados_cel.to_excel(writer, sheet_name='Duplicados_Celular', index=False)
        quitar_negrita_excel(ruta_incorrecta)
        print(f"Archivo INCORRECTA creado: {ruta_incorrecta}")
    except Exception:
        print(f"Error creando archivo INCORRECTA")
    sys.exit("✅ Proceso detenido por registros incompletos o duplicados.")

df['celular'] = df['celular_norm']
df.drop(columns=['celular_norm'], inplace=True)


# ==============================
# 7️⃣ Añadir año, mes y texto extraído
# ==============================
df['año'] = fecha_actual.year
df['mes'] = mes_actual
df['texto_extraido'] = fecha_actual.strftime("%d%b%Y").lower()
cols = ['año', 'mes', 'texto_extraido'] + [c for c in df.columns if c not in ['año','mes','texto_extraido']]
df = df[cols]

if 'id_cliente' in df.columns:
    df = df.drop_duplicates(subset=['id_cliente'])
    print(f"Total registros únicos por cliente: {len(df)}")

# ==============================
# 8️⃣ Guardar CORRECTA
# ==============================
ruta_correcta = os.path.join(carpeta_base, f"CORRECTA_{mes_actual}.xlsx")
try:
    df.to_excel(ruta_correcta, index=False)
    quitar_negrita_excel(ruta_correcta)
    print(f"✅ Archivo CORRECTA guardado: {ruta_correcta}")
except Exception:
    print(f"Error guardando archivo CORRECTA")

# ==============================
# 9️⃣ Ejecutar cargarpre.py automáticamente
# ==============================
ruta_cargarpre = r"C:\Users\pasante.ti2\Desktop\cargarBases-20250917T075622Z-1-001\cargarBases\cargarpre.py"
if os.path.exists(ruta_correcta) and os.path.exists(ruta_cargarpre):
    try:
        resultado = subprocess.run([sys.executable, ruta_cargarpre], capture_output=True, text=True)
        print(resultado.stdout)
        print(resultado.stderr)
        if resultado.returncode == 0:
            print("✅ cargarpre.py ejecutado correctamente.")
        else:
            print(f"Error al ejecutar cargarpre.py (código {resultado.returncode})")
    except Exception as e:
        print(f"Error ejecutando cargarpre.py: {e}")
else:
    print("No se ejecuta cargarpre.py: CORRECTA o cargarpre.py no encontrado.")