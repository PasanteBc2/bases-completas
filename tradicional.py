import pandas as pd # Manejo de datos
import glob # Búsqueda de archivos
import os # Manejo de rutas
import sys # Salida del sistema 
from sqlalchemy import create_engine, text # Conexión a la base de datos
from sqlalchemy.engine import URL # URL de conexión
from sqlalchemy.exc import OperationalError # Manejo de errores de conexión
import logging # Registro de logs

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ========= Conexión a la base de datos (PostgreSQL) =========
usuario = "analista"
contraseña = "2025Anal1st@"   
host = "192.168.10.37"
puerto = 5432
base_datos = "tradicional"

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

# ==============================
# 2️⃣ Leer todos los Excel
# ==============================
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
            # Normalizar y limpiar columnas

            df_hoja['año'] = '2025'
            df_hoja['mes'] = df_hoja.get('mes', mes_carpeta).fillna(mes_carpeta).str.strip().str.upper()
            df_hoja['texto_extraido'] = df_hoja.get('texto_extraido','').apply(lambda x: x.strip() if pd.notna(x) else '')

            for campo in ['identificacion','nombre_completo','celular']:
                df_hoja[campo] = df_hoja.get(campo,'').apply(lambda x: x.strip() if pd.notna(x) else '')

        # Normalizar celular
            df_hoja['celular'] = df_hoja['celular'].astype(str).str.replace(r'\.0$', '', regex=True)
            df_hoja['celular'] = df_hoja['celular'].apply(lambda x: x if x.startswith('0') else '0'+x if x and x.isdigit() else x)

            for campo in ['operadora_destino','deuda_movistar','dpa_provincia']:
                if campo not in df_hoja.columns:
                    df_hoja[campo] = ''
                else:
                    df_hoja[campo] = df_hoja[campo].fillna('').astype(str)

            df_list.append(df_hoja)

        total_registros = sum(len(df_hoja) for df_hoja in hojas.values())
        print(f"✅ Leído {nombre_archivo} ({total_registros} filas) con mes {mes_carpeta}")
    except Exception as e:
        print(f"⚠️ Error leyendo {nombre_archivo}: {e}")

if not df_list:
    sys.exit("❌ No se pudo leer ningún Excel correctamente.")

# ==============================
# 3️⃣ Concatenar y normalizar columnas
# ==============================
columnas_finales = ['año','mes','texto_extraido','identificacion','nombre_completo','celular','dpa_provincia','operadora_destino','deuda_movistar']
df_limpios = []
for df_hoja in df_list:
    for c in columnas_finales:
        if c not in df_hoja.columns:
            df_hoja[c] = ''
    df_hoja = df_hoja[columnas_finales]
    df_limpios.append(df_hoja)

df = pd.concat(df_limpios, ignore_index=True)
print(f"📊 Total registros combinados: {len(df)}")

# ==============================
# 4️⃣ Obtener IDs de años y meses
# ==============================
anio_db = pd.read_sql("SELECT id_anio, valor FROM anio", engine)
anio_db['valor'] = anio_db['valor'].astype(str).str.strip()
mes_db = pd.read_sql("SELECT id_mes, nombre_mes FROM mes", engine)
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
df_periodos_db = pd.read_sql("SELECT * FROM periodo_carga", engine)
periodo_map = df_periodos_db.set_index(['id_anio','id_mes','texto_extraido'])['id_periodo'].to_dict()
df['id_periodo'] = df.apply(lambda row: periodo_map.get((row['id_anio'], row['id_mes'], row['texto_extraido'])), axis=1)
print(f"❗ Filas sin id_periodo: {df['id_periodo'].isna().sum()}")

# ==============================
# 6️⃣ Insertar provincias automáticamente
# ==============================
prov_db = pd.read_sql("SELECT id_provincia, nombre_provincia FROM provincia", engine)
prov_excel = df['dpa_provincia'].str.strip().str.upper().dropna().unique()
prov_nuevas = [p for p in prov_excel if p not in prov_db['nombre_provincia'].str.upper().values]

if prov_nuevas:
    pd.DataFrame({'nombre_provincia': prov_nuevas}).to_sql('provincia', engine, if_exists='append', index=False, method='multi')
    print(f"🌍 Provincias agregadas: {prov_nuevas}")

prov_db = pd.read_sql("SELECT id_provincia, nombre_provincia FROM provincia", engine)
prov_map = dict(zip(prov_db['nombre_provincia'].str.upper(), prov_db['id_provincia']))
df['id_provincia'] = df['dpa_provincia'].str.upper().map(prov_map)

# ==============================
# 8️⃣ Insertar clientes en bloques y obtener id_cliente
# ==============================
id_cliente_list = []
bloque = 500
with engine.begin() as conn:
    for i in range(0, len(df), bloque):
        batch = df.iloc[i:i+bloque]
        for _, row in batch.iterrows():
            result = conn.execute(
                text("""
                    INSERT INTO cliente (identificacion,nombre_completo,celular,id_provincia,operadora_destino,deuda_movistar)
                    VALUES (:identificacion,:nombre_completo,:celular,:id_provincia,:operadora_destino,:deuda_movistar)
                    RETURNING id_cliente
                """),
                {
                    'identificacion': row['identificacion'],
                    'nombre_completo': row['nombre_completo'],
                    'celular': row['celular'],
                    'id_provincia': row['id_provincia'],
                    'operadora_destino': row.get('operadora_destino','NO REGISTRA'),
                    'deuda_movistar': row.get('deuda_movistar','')
                }
            )
            id_cliente_list.append(result.scalar())

df['id_cliente'] = id_cliente_list
print(f"✅ Clientes insertados correctamente: {len(df)}")

# ==============================
# 9️⃣ Insertar cliente_plan_info en bloques
# ==============================
with engine.begin() as conn:
    for i in range(0, len(df), bloque):
        batch = df.iloc[i:i+bloque]
        for _, row in batch.iterrows():
            if pd.notna(row['id_cliente']) and pd.notna(row['id_periodo']):
                conn.execute(
                    text("""
                        INSERT INTO cliente_plan_info (id_cliente,id_periodo)
                        VALUES (:id_cliente,:id_periodo)
                    """),
                    {'id_cliente': row['id_cliente'], 'id_periodo': row['id_periodo']}
                )

print(f"✅ Cliente_plan_info insertados correctamente: {len(df)}")
