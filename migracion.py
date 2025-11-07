import pandas as pd
import glob
import os
import sys
from sqlalchemy import create_engine

# ==============================
# 1️⃣ Conexión a PostgreSQL
# ==============================
usuario = 'postgres'
contraseña = 'pasante'
host = 'localhost'
puerto = '5432'
base_datos = 'migracion'

connection_string = f'postgresql+psycopg2://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}'
engine = create_engine(connection_string)

# ==============================
# 2️⃣ Leer todos los Excel de la carpeta
# ==============================
carpeta_principal = r'C:\Users\pasante.ti2\Documents\MOVISTAR\MIGRACION'
rutas_excel = glob.glob(os.path.join(carpeta_principal, '**', '*.xlsx'), recursive=True)
if not rutas_excel:
    sys.exit("❌ No se encontraron archivos Excel en la carpeta indicada.")

df_list = []

# ==============================
# 2️⃣ Leer hojas y mantener datos exactos
# ==============================
for ruta_excel in rutas_excel:
    nombre_archivo = os.path.basename(ruta_excel)
    if nombre_archivo.startswith('~$'):
        continue
    
    carpeta_relativa = os.path.relpath(os.path.dirname(ruta_excel), carpeta_principal)
    mes_carpeta = carpeta_relativa.split(os.sep)[0].upper()

    try:
        hojas = pd.read_excel(ruta_excel, sheet_name=None, dtype=str)
        for nombre_hoja, df_hoja in hojas.items():
            df_hoja.columns = [col.lower().strip() for col in df_hoja.columns]

            # Mantener mes
            df_hoja['mes'] = df_hoja.get('mes', mes_carpeta).fillna(mes_carpeta).astype(str).str.strip().str.upper()

            # Texto extraído
            df_hoja['texto_extraido'] = df_hoja.get('texto_extraido', '').apply(lambda x: x.strip() if pd.notna(x) else '')

            # Identificación, nombre completo y celular
            for campo in ['identificacion', 'nombre_completo', 'celular']:
                df_hoja[campo] = df_hoja.get(campo, '').apply(lambda x: x.strip() if pd.notna(x) else '')

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
        print(f"⚠️ Error leyendo {nombre_archivo}: {e}")

if not df_list:
    sys.exit("❌ No se pudo leer ningún Excel correctamente.")

# ==============================
# 🔧 Normalizar columnas antes de concatenar
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
print(f"📊 Total registros combinados (limpios): {len(df)}")

# ==============================
# 3️⃣ Normalizar columnas adicionales
# ==============================
df['año'] = '2025'
df['mes'] = df['mes'].str.replace(r'^\d{2}\.', '', regex=True).str.upper()

# ==============================
# 4️⃣ Obtener IDs de años y meses
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
# 5️⃣ Insertar periodos únicos
# ==============================
df_periodos = df[['id_anio','id_mes','texto_extraido']].drop_duplicates()
df_periodos.to_sql('periodo_carga', engine, if_exists='append', index=False, method='multi')
print(f"✅ Periodos únicos insertados: {len(df_periodos)}")

# ==============================
# 6️⃣ Asignar id_periodo
# ==============================
df_periodos_db = pd.read_sql('SELECT * FROM periodo_carga', engine)
periodo_map = df_periodos_db.set_index(['id_anio','id_mes','texto_extraido'])['id_periodo'].to_dict()
df['id_periodo'] = df.apply(lambda row: periodo_map.get((row['id_anio'], row['id_mes'], row['texto_extraido'])), axis=1)

# ==============================
# 7️⃣ Insertar provincias automáticamente
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
# 8️⃣ Insertar clientes con id_provincia tal cual
# ==============================
df['id_provincia'] = df['dpa_provincia'].str.strip().str.upper().map(prov_map)
df_cliente = df[['identificacion','nombre_completo','celular','id_provincia']].copy()
df_cliente.to_sql('cliente', engine, if_exists='append', index=False, method='multi')
print(f"✅ Clientes insertados: {len(df_cliente)}")

# ==============================
# 9️⃣ Asignar id_cliente
# ==============================
df_clientes_db = pd.read_sql('SELECT id_cliente, identificacion, nombre_completo, celular FROM cliente', engine)
excel_keys = list(zip(df['identificacion'], df['nombre_completo'], df['celular']))
db_keys = list(zip(df_clientes_db['identificacion'], df_clientes_db['nombre_completo'], df_clientes_db['celular']))
id_cliente_map = dict(zip(db_keys, df_clientes_db['id_cliente']))
df['id_cliente'] = [id_cliente_map.get(k) for k in excel_keys]

# ==============================
# 🔟 Insertar cliente_plan_info (sin tocar provincias)
# ==============================
df_plan_info = df[['id_cliente','id_periodo','tbs','decil_online','decil_pago']].copy()
df_plan_info.to_sql('cliente_plan_info', engine, if_exists='append', index=False, method='multi')
print(f"✅ Cliente_plan_info insertados: {len(df_plan_info)}")

# ==============================
# 1️⃣1️⃣ Resumen final
# ==============================
resumen_mes = df.groupby('mes').size().reset_index(name='registros')
print("\n📊 Registros por mes:")
print(resumen_mes)

print("\n🎉 ¡Carga completa en PostgreSQL con todos los registros exactos y relaciones de provincia correctas!")
