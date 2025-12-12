import pandas as pd # pip install pandas
import glob # Para buscar archivos en carpetas
import os # Para manejar rutas de archivos
import sys # Para salir del script en caso de error
from sqlalchemy import create_engine # pip install sqlalchemy psycopg2-binary
from sqlalchemy.engine import URL # Para construir la URL de conexión
from sqlalchemy.exc import OperationalError # Para manejar errores de conexión
import logging # Para registrar eventos


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ========= Conexión a la base de datos (PostgreSQL) =========
usuario = "analista"
contraseña = "2025Anal1st@"   # Déjala tal cual; URL.create la escapa
host = "192.168.10.37"
puerto = 5432
base_datos = "BcorpDigitalPrueba"


# Requiere: pip install psycopg2-binary
url = URL.create(
    drivername="postgresql+psycopg2",
    username=usuario,
    password=contraseña,
    host=host,
    port=puerto,
    database=base_datos,
)

try:
    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_timeout=60,
    )
    with engine.connect() as conn:
        logging.info("✅ Conexión a PostgreSQL OK.")
except OperationalError as e:
    logging.exception("❌ Error de conexión a PostgreSQL.")
    raise SystemExit(e)




# ==============================
# 2️⃣ Leer todos los Excel de la carpeta
# ==============================
carpeta_principal = r'C:\Users\pasante.ti2\Documents\Movistar\DIGITAL'
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
            if 'mes' not in df_hoja.columns:
                df_hoja['mes'] = mes_carpeta
            else:
                df_hoja['mes'] = df_hoja['mes'].fillna(mes_carpeta).astype(str).str.strip().str.upper()

            # Mantener texto_extraido exactamente como en Excel
            if 'texto_extraido' not in df_hoja.columns:
                df_hoja['texto_extraido'] = ''
            else:
                df_hoja['texto_extraido'] = df_hoja['texto_extraido'].apply(lambda x: x.strip() if pd.notna(x) else '')

            # Mantener identificacion y nombre_completo exactos
            if 'identificacion' not in df_hoja.columns:
                df_hoja['identificacion'] = ''
            else:
                df_hoja['identificacion'] = df_hoja['identificacion'].apply(lambda x: x.strip() if pd.notna(x) else '')

            if 'nombre_completo' not in df_hoja.columns:
                df_hoja['nombre_completo'] = ''
            else:
                df_hoja['nombre_completo'] = df_hoja['nombre_completo'].apply(lambda x: x.strip() if pd.notna(x) else '')

            # Celular
            if 'celular' in df_hoja.columns:
                df_hoja['celular'] = df_hoja['celular'].astype(str).str.replace(r'\.0$', '', regex=True)
                df_hoja['celular'] = df_hoja['celular'].apply(lambda x: x if x.startswith('0') else '0'+x)

            df_list.append(df_hoja)

        total_registros = sum(len(df_hoja) for df_hoja in hojas.values())
        print(f"✅ Leído {nombre_archivo} ({total_registros} filas) con mes {mes_carpeta}")
    except Exception as e:
        print(f"⚠️ Error leyendo {nombre_archivo}: {e}")

if not df_list:
    sys.exit("❌ No se pudo leer ningún Excel correctamente.")

df = pd.concat(df_list, ignore_index=True)
print(f"📊 Total registros combinados: {len(df)}")

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
# 6️⃣ Asignar id_periodo con diccionario
# ==============================
df_periodos_db = pd.read_sql('SELECT * FROM periodo_carga', engine)
periodo_map = df_periodos_db.set_index(['id_anio','id_mes','texto_extraido'])['id_periodo'].to_dict()
df['id_periodo'] = df.apply(lambda row: periodo_map.get((row['id_anio'], row['id_mes'], row['texto_extraido'])), axis=1)

# ==============================
# 7️⃣ Insertar clientes EXACTAMENTE como en Excel
# ==============================
df_cliente = df[['identificacion','nombre_completo','celular']].copy()
df_cliente.to_sql('cliente', engine, if_exists='append', index=False, method='multi')
print(f"✅ Clientes insertados: {len(df_cliente)}")  # 57297

# ==============================
# 8️⃣ Asignar id_cliente fila a fila sin duplicar
# ==============================
df_clientes_db = pd.read_sql('SELECT id_cliente, identificacion, nombre_completo, celular FROM cliente', engine)
excel_keys = list(zip(df['identificacion'], df['nombre_completo'], df['celular']))
db_keys = list(zip(df_clientes_db['identificacion'], df_clientes_db['nombre_completo'], df_clientes_db['celular']))
id_cliente_map = dict(zip(db_keys, df_clientes_db['id_cliente']))
df['id_cliente'] = [id_cliente_map.get(k) for k in excel_keys]

# ==============================
# 9️⃣ Insertar cliente_plan_info EXACTAMENTE igual al número de filas original
# ==============================
df_plan_info = df[['id_cliente','id_periodo']].copy()  # No drop_duplicates
df_plan_info.to_sql('cliente_plan_info', engine, if_exists='append', index=False, method='multi')
print(f"✅ Cliente_plan_info insertados: {len(df_plan_info)}")  # 57297 ✅

# ==============================
# 🔟 Resumen final por mes
# ==============================
resumen_mes = df.groupby('mes').size().reset_index(name='registros')
print("\n📊 Registros por mes:")
print(resumen_mes)

print("\n🎉 ¡Carga completa en PostgreSQL con todos los registros exactos y vacíos respetados!")
