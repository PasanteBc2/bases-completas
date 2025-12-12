import sys
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError
import logging
import tkinter as tk
from tkinter import filedialog

# ==============================
# 1️⃣ Configuración PostgreSQL
# ==============================
usuario = 'postgres'
contraseña = 'pasante'
host = 'localhost'
puerto = '5432'
base_datos = 'migracion'

engine = create_engine(f'postgresql+psycopg2://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}')

# ==============================
# 2️⃣ Seleccionar un archivo Excel
# ==============================
root = tk.Tk()
root.withdraw()  # Ocultar ventana principal
ruta_excel = filedialog.askopenfilename(
    title="Seleccione un archivo Excel",
    filetypes=[("Archivos Excel", "*.xlsx *.xls")]
)

if not ruta_excel:
    raise SystemExit("❌ No se seleccionó ningún archivo.")

nombre_archivo = os.path.basename(ruta_excel)
carpeta_principal = os.path.dirname(ruta_excel)
mes_carpeta = os.path.basename(carpeta_principal).upper()  # Tomar carpeta como mes

# ==============================
# 3️⃣ Leer hojas del Excel
# ==============================
df_list = []
try:
    hojas = pd.read_excel(ruta_excel, sheet_name=None, dtype=str)
    for nombre_hoja, df_hoja in hojas.items():
        df_hoja.columns = [col.lower().strip() for col in df_hoja.columns]

        # Mantener mes
        if 'mes' not in df_hoja.columns:
            df_hoja['mes'] = mes_carpeta
        else:
            df_hoja['mes'] = df_hoja['mes'].fillna(mes_carpeta).astype(str).str.strip().str.upper()

        # Texto extraído
        if 'texto_extraido' not in df_hoja.columns:
            df_hoja['texto_extraido'] = ''
        else:
            df_hoja['texto_extraido'] = df_hoja['texto_extraido'].apply(lambda x: x.strip() if pd.notna(x) else '')

        # Identificación, nombre completo y celular
        for campo in ['identificacion', 'nombre_completo', 'celular']:
            if campo not in df_hoja.columns:
                df_hoja[campo] = ''
            else:
                df_hoja[campo] = df_hoja[campo].apply(lambda x: x.strip() if pd.notna(x) else '')

        # Celular: limpiar y asegurar que empiece con 0
        df_hoja['celular'] = df_hoja['celular'].astype(str).str.replace(r'\.0$', '', regex=True)
        df_hoja['celular'] = df_hoja['celular'].apply(lambda x: x if x.startswith('0') else '0'+x if x and x.isdigit() else x)

        # Campos adicionales
        for campo in ['tbs', 'decil_online', 'decil_pago', 'dpa_provincia']:
            if campo not in df_hoja.columns:
                df_hoja[campo] = ''

        df_list.append(df_hoja)

    total_registros = sum(len(df_hoja) for df_hoja in hojas.values())
    print(f"✅ Leído {nombre_archivo} ({total_registros} filas) con mes {mes_carpeta}")
except Exception as e:
    raise SystemExit(f"⚠️ Error leyendo {nombre_archivo}: {e}")

# ==============================
# 4️⃣ Normalizar columnas antes de concatenar
# ==============================
columnas_finales = [
    'año', 'mes', 'texto_extraido',
    'nombre_completo', 'identificacion', 'celular',
    'tbs', 'decil_online', 'decil_pago', 'dpa_provincia'
]

df_limpios = []
for df_hoja in df_list:
    df_hoja.columns = [col.lower().strip().split('_m')[0] for col in df_hoja.columns]
    for c in columnas_finales:
        if c not in df_hoja.columns:
            df_hoja[c] = ''
    df_hoja = df_hoja[columnas_finales]
    df_limpios.append(df_hoja)

df = pd.concat(df_limpios, ignore_index=True)
df = df.fillna('')  # 🔹 Mantener vacíos tal cual
print(f"📊 Total registros combinados (limpios): {len(df)}")

# ==============================
# 5️⃣ Normalizar columnas adicionales
# ==============================
df['año'] = '2025'
df['mes'] = df['mes'].str.replace(r'^\d{2}\.', '', regex=True).str.upper()

# ==============================
# 6️⃣ Obtener IDs de años y meses
# ==============================
anio_db = pd.read_sql('SELECT id_anio, valor FROM anio', engine)
anio_db['valor'] = anio_db['valor'].astype(str).str.strip()
mes_db = pd.read_sql('SELECT id_mes, nombre_mes FROM mes', engine)
mes_db['nombre_mes'] = mes_db['nombre_mes'].astype(str).str.strip().str.upper()

df = df.merge(anio_db, left_on='año', right_on='valor', how='left')
df = df.merge(mes_db, left_on='mes', right_on='nombre_mes', how='left')

if df['id_anio'].isnull().any() or df['id_mes'].isnull().any():
    sys.exit("❌ Hay años o meses que no existen en la DB.")

# ==============================
# 7️⃣ Insertar periodos únicos
# ==============================
df_periodos = df[['id_anio','id_mes','texto_extraido']].drop_duplicates().fillna('')
df_periodos.to_sql('periodo_carga', engine, if_exists='append', index=False, method='multi')
print(f"✅ Periodos únicos insertados: {len(df_periodos)}")

# ==============================
# 8️⃣ Asignar id_periodo
# ==============================
df_periodos_db = pd.read_sql('SELECT * FROM periodo_carga', engine)
periodo_map = df_periodos_db.set_index(['id_anio','id_mes','texto_extraido'])['id_periodo'].to_dict()
df['id_periodo'] = df.apply(lambda row: periodo_map.get((row['id_anio'], row['id_mes'], row['texto_extraido'])), axis=1)

# ==============================
# 9️⃣ Insertar provincias automáticamente
# ==============================
prov_db = pd.read_sql('SELECT id_provincia, nombre_provincia FROM provincia', engine)
prov_excel = df['dpa_provincia'].str.strip().str.upper().dropna().unique()
prov_nuevas = [p for p in prov_excel if p not in prov_db['nombre_provincia'].str.upper().values]

if prov_nuevas:
    df_prov_nuevas = pd.DataFrame({'nombre_provincia': prov_nuevas})
    df_prov_nuevas.to_sql('provincia', engine, if_exists='append', index=False, method='multi')

# Actualizar mapeo final
prov_db = pd.read_sql('SELECT id_provincia, nombre_provincia FROM provincia', engine)
prov_map = dict(zip(prov_db['nombre_provincia'].str.upper(), prov_db['id_provincia']))

# ==============================
# 🔟 Insertar clientes
# ==============================
df['id_provincia'] = df['dpa_provincia'].str.strip().str.upper().map(prov_map)
df_cliente = df[['identificacion','nombre_completo','celular','id_provincia']].copy()
df_cliente = df_cliente.fillna('')  # 🔹 Mantener vacíos
df_cliente.to_sql('cliente', engine, if_exists='append', index=False, method='multi')
print(f"✅ Clientes insertados: {len(df_cliente)}")

# ==============================
# 1️⃣1️⃣ Asignar id_cliente
# ==============================
df_clientes_db = pd.read_sql('SELECT id_cliente, identificacion, nombre_completo, celular FROM cliente', engine)
excel_keys = list(zip(df['identificacion'], df['nombre_completo'], df['celular']))
db_keys = list(zip(df_clientes_db['identificacion'], df_clientes_db['nombre_completo'], df_clientes_db['celular']))
id_cliente_map = dict(zip(db_keys, df_clientes_db['id_cliente']))
df['id_cliente'] = [id_cliente_map.get(k) for k in excel_keys]

# ==============================
# 1️⃣2️⃣ Insertar cliente_plan_info
# ==============================
df_plan_info = df[['id_cliente','id_periodo','tbs','decil_online','decil_pago']].copy()
df_plan_info = df_plan_info.fillna('')  # 🔹 Mantener vacíos
df_plan_info.to_sql('cliente_plan_info', engine, if_exists='append', index=False, method='multi')
print(f"✅ Cliente_plan_info insertados: {len(df_plan_info)}")

# ==============================
# 1️⃣3️⃣ Resumen final
# ==============================
resumen_mes = df.groupby('mes').size().reset_index(name='registros')
print("\n📊 Registros por mes:")
print(resumen_mes)

print("\n🎉 ¡Carga completa en PostgreSQL con todos los registros exactos y valores vacíos intactos!")
