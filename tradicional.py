import pandas as pd
import glob
import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==========================================
# 1️⃣ CONEXIÓN A LA BD
# ==========================================
usuario = "analista"
contraseña = "2025Anal1st@"
host = "192.168.10.37"
puerto = 5432
base_datos = "BcorpTradicionalPrueba"

url = URL.create(
    drivername="postgresql+psycopg2",
    username=usuario,
    password=contraseña,
    host=host,
    port=puerto,
    database=base_datos,
)

try:
    engine = create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10, pool_timeout=60)
    with engine.connect() as conn:
        logging.info("✅ Conexión a PostgreSQL OK.")
except OperationalError as e:
    logging.exception("❌ Error de conexión a PostgreSQL.")
    raise SystemExit(e)

# ==========================================
# 2️⃣ LEER TODOS LOS EXCEL DE TRADICIONAL
# ==========================================
carpeta_principal = r'C:\Users\pasante.ti2\Documents\Movistar\TRADICIONAL'
rutas_excel = glob.glob(os.path.join(carpeta_principal, '**', '*.xlsx'), recursive=True)

if not rutas_excel:
    sys.exit("❌ No se encontraron archivos Excel en la carpeta indicada.")

df_list = []

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

            columnas_obligatorias = ['texto_extraido','identificacion','nombre_completo','celular']
            for c in columnas_obligatorias:
                if c not in df_hoja.columns:
                    df_hoja[c] = ""

            df_hoja['año'] = '2025'
            df_hoja['mes'] = df_hoja.get('mes', mes_carpeta).fillna(mes_carpeta).astype(str).str.upper()

            df_hoja['texto_extraido'] = df_hoja['texto_extraido'].astype(str).str.strip()
            df_hoja['identificacion']  = df_hoja['identificacion'].astype(str).str.strip()
            df_hoja['nombre_completo'] = df_hoja['nombre_completo'].astype(str).str.strip()

            df_hoja['celular'] = (
                df_hoja['celular'].astype(str)
                    .str.replace(r"\.0$", "", regex=True)
                    .apply(lambda x: "0"+x if x.isdigit() and not x.startswith("0") else x)
            )

            for campo in ['operadora_destino','deuda_movistar','dpa_provincia']:
                df_hoja[campo] = df_hoja.get(campo, '').fillna('').astype(str)

            df_list.append(df_hoja)

        print(f"✅ Leído {nombre_archivo} ({sum(len(x) for x in hojas.values())} filas) con mes {mes_carpeta}")

    except Exception as e:
        print(f"⚠️ Error leyendo {nombre_archivo}: {e}")

if not df_list:
    sys.exit("❌ No se pudo leer ningún Excel correctamente.")

# ==========================================
# 3️⃣ UNIFICAR COLUMNAS
# ==========================================
columnas_finales = [
    'año','mes','texto_extraido','identificacion','nombre_completo',
    'celular','dpa_provincia','operadora_destino','deuda_movistar'
]

df_limpios = []
for df_hoja in df_list:
    for c in columnas_finales:
        if c not in df_hoja.columns:
            df_hoja[c] = ""
    df_limpios.append(df_hoja[columnas_finales])

df = pd.concat(df_limpios, ignore_index=True)
print(f"📊 Total registros combinados: {len(df)}")

# ==========================================
# 4️⃣ OBTENER ID AÑO Y MES
# ==========================================
anio_db = pd.read_sql("SELECT id_anio, valor FROM anio", engine)
anio_db['valor'] = anio_db['valor'].astype(str).str.strip()

mes_db = pd.read_sql("SELECT id_mes, nombre_mes FROM mes", engine)
mes_db['nombre_mes'] = mes_db['nombre_mes'].astype(str).str.upper().str.strip()

df = df.merge(anio_db, left_on='año', right_on='valor', how='left')
df = df.merge(mes_db, left_on='mes', right_on='nombre_mes', how='left')

if df['id_anio'].isnull().any() or df['id_mes'].isnull().any():
    print(df[df['id_mes'].isnull()][['mes']].drop_duplicates())
    sys.exit("❌ Hay años o meses que no existen en la DB.")

# ==========================================
# 5️⃣ INSERTAR PERIODOS ÚNICOS
# ==========================================
df_periodos = df[['id_anio','id_mes','texto_extraido']].drop_duplicates()
df_periodos.to_sql('periodo_carga', engine, if_exists='append', index=False, method='multi')

df_periodos_db = pd.read_sql("SELECT * FROM periodo_carga", engine)
periodo_map = df_periodos_db.set_index(['id_anio','id_mes','texto_extraido'])['id_periodo'].to_dict()
df['id_periodo'] = df.apply(lambda row: periodo_map.get((row['id_anio'], row['id_mes'], row['texto_extraido'])), axis=1)

# ==========================================
# 6️⃣ INSERTAR PROVINCIAS NUEVAS
# ==========================================
prov_db = pd.read_sql("SELECT id_provincia, nombre_provincia FROM provincia", engine)
prov_excel = df['dpa_provincia'].str.strip().str.upper().dropna().unique()

prov_nuevas = [p for p in prov_excel if p not in prov_db['nombre_provincia'].str.upper().values]

if prov_nuevas:
    pd.DataFrame({'nombre_provincia': prov_nuevas}) \
        .to_sql('provincia', engine, if_exists='append', index=False, method='multi')

prov_db = pd.read_sql("SELECT id_provincia, nombre_provincia FROM provincia", engine)
prov_map = dict(zip(prov_db['nombre_provincia'].str.upper(), prov_db['id_provincia']))

df['id_provincia'] = df['dpa_provincia'].str.upper().map(prov_map)

# -------------------------
# 7️⃣ INSERTAR CLIENTES (SIN PERDER NINGÚN REGISTRO)
# -------------------------

# 🔥 Eliminar cualquier columna vieja llamada id_cliente para evitar IDs 20001+
df = df.drop(columns=[col for col in df.columns if col.lower().startswith("id_cliente")], errors="ignore")

# Cliente = CADA FILA, SIN ELIMINAR DUPLICADOS
df_cliente = df[['identificacion','nombre_completo','celular','id_provincia',
                 'operadora_destino','deuda_movistar']].copy()

# Insertar todos los clientes EXACTOS del Excel
df_cliente.to_sql('cliente', engine, if_exists='append', index=False, method='multi')
print(f"✅ Clientes insertados (SIN eliminar ninguno): {len(df_cliente)}")


# ------------------------------------------
# 8️⃣ 🔥 REASIGNAR ID_CLIENTE 
# ------------------------------------------

cantidad = len(df_cliente)

# 1️⃣ Obtener los últimos IDs generados por PostgreSQL (en el orden correcto)
df_ids = pd.read_sql(f"""
    SELECT id_cliente
    FROM cliente
    ORDER BY id_cliente DESC
    LIMIT {cantidad}
""", engine)

# Ordenarlos para que coincidan con el Excel
df_ids = df_ids.sort_values('id_cliente').reset_index(drop=True)

# 2️⃣ Asignar esos IDs reales al dataframe principal
df['id_cliente'] = df_ids['id_cliente']

print("🔥 ID_CLIENTE asignado correctamente según PostgreSQL")


# -------------------------
# 9️⃣ INSERTAR cliente_plan_info
# -------------------------

df_plan_info = df[['id_cliente','id_periodo']].copy()

df_plan_info.to_sql(
    'cliente_plan_info',
    engine,
    if_exists='append',
    index=False,
    method='multi'
)

print(f"✅ cliente_plan_info insertados: {len(df_plan_info)}")
