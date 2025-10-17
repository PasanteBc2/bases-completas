import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import os
import glob
import sys
import traceback

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
        print("  Conexión a PostgreSQL establecida correctamente.")
except OperationalError as e:
    sys.exit(f"No se pudo conectar a la base de datos: {e}")
except Exception as e:
    sys.exit(f"Error inesperado al conectar a la base de datos: {e}")

# ==============================
# 2️⃣ Detectar archivo CORRECTA
# ==============================
carpeta_excel = r"C:\Users\pasante.ti2\Desktop\bases prepago"
try:
    archivos = glob.glob(os.path.join(carpeta_excel, "CORRECTA_*.xlsx"))
    if not archivos:
        archivos = glob.glob(os.path.join(carpeta_excel, "*CORRECTA*.xlsx"))
        if not archivos:
            sys.exit("No se encontró ningún archivo 'CORRECTA' en la carpeta de prepago.")
    ruta_excel = max(archivos, key=os.path.getmtime)
    print(f"  Archivo CORRECTA detectado: {ruta_excel}")
except Exception as e:
    sys.exit(f"Error detectando archivos CORRECTA: {e}")

# ==============================
# 3️⃣ Leer Excel
# ==============================
try:
    df = pd.read_excel(ruta_excel)
    df.columns = [c.lower().strip() for c in df.columns]
    print(f" Hoja leída con {len(df)} registros y {len(df.columns)} columnas.")
except Exception as e:
    sys.exit(f"Error leyendo Excel: {e}")

# ==============================
# 4️⃣ Normalización y limpieza
# ==============================
df = df.fillna('')
df.rename(columns={'año': 'anio'}, inplace=True)
for col in ['anio','mes','texto_extraido','nombre_completo','monto_recarga','identificacion','celular']:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

def limpiar_identificacion(valor):
    valor = str(valor).strip()
    if valor.endswith('.0'):
        try:
            return str(int(float(valor)))
        except ValueError:
            return valor
    return valor

df['identificacion'] = df['identificacion'].apply(limpiar_identificacion)
df.loc[df['identificacion'] == '', 'identificacion'] = '9999999999'

def normalizar_celular(valor):
    valor = str(valor).strip()
    if valor == '':
        return ''
    try:
        return str(int(float(valor))).zfill(10)
    except ValueError:
        return valor

df['celular'] = df['celular'].apply(normalizar_celular)

# ==============================
# 5️⃣ Funciones para SQL con transacción y excepciones
# ==============================
def ejecutar_sql(sql, params=None):
    try:
        with engine.begin() as conn:  # Maneja commit/rollback automáticamente
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)
        return True
    except SQLAlchemyError as e:
        print(f"Error ejecutando SQL: {e}")
        return False

def leer_sql(query):
    try:
        return pd.read_sql(query, engine)
    except SQLAlchemyError as e:
        print(f"Error leyendo SQL: {e}")
        return pd.DataFrame()

# ==============================
# 6️⃣ Cargar tablas de referencia
# ==============================
df_anio = leer_sql('SELECT * FROM anio')
df_mes = leer_sql('SELECT * FROM mes')

df = df.merge(df_anio, left_on='anio', right_on='valor', how='left')
df = df.merge(df_mes, left_on='mes', right_on='nombre_mes', how='left')

if df['id_anio'].isnull().any() or df['id_mes'].isnull().any():
    sys.exit("Error: Algunos registros tienen año o mes inválido.")

# ==============================
# 7️⃣ Insertar nuevos periodos
# ==============================
df_periodos = df[['id_anio','id_mes','texto_extraido']].drop_duplicates()
periodos_existentes = leer_sql('SELECT id_anio,id_mes,texto_extraido FROM periodo_carga')
df_nuevos_periodos = df_periodos.merge(periodos_existentes, on=['id_anio','id_mes','texto_extraido'], how='left', indicator=True)
df_nuevos_periodos = df_nuevos_periodos[df_nuevos_periodos['_merge']=='left_only'].drop(columns=['_merge'])

if not df_nuevos_periodos.empty:
    print(f"Insertando {len(df_nuevos_periodos)} nuevos periodos...")
    try:
        df_nuevos_periodos.to_sql('periodo_carga', engine, if_exists='append', index=False)
    except SQLAlchemyError as e:
        sys.exit(f"Error insertando nuevos periodos: {e}")
else:
    print("No hay nuevos periodos.")

# Refrescar periodo_carga
df_periodos_actualizados = leer_sql('SELECT id_periodo,id_anio,id_mes,texto_extraido FROM periodo_carga')
df = df.merge(df_periodos_actualizados, on=['id_anio','id_mes','texto_extraido'], how='left')

# ==============================
# 8️⃣ Insertar clientes
# ==============================
df_clientes = df[['identificacion','celular','monto_recarga','nombre_completo']].copy()
print(f"Insertando {len(df_clientes)} registros en tabla cliente...")

try:
    df_clientes.to_sql('cliente', engine, if_exists='append', index=False)
except SQLAlchemyError as e:
    sys.exit(f"Error insertando clientes: {e}")

# ==============================
# 9️⃣ Asociar id_cliente
# ==============================
clientes_totales = leer_sql('SELECT id_cliente,identificacion,celular,nombre_completo FROM cliente')
n = len(df_clientes)
clientes_nuevos = clientes_totales.tail(n).copy()

df_clientes = df_clientes.reset_index(drop=True)
clientes_nuevos = clientes_nuevos.reset_index(drop=True)
df_clientes['idx'] = df_clientes.index
clientes_nuevos['idx'] = clientes_nuevos.index

df_merged = pd.merge(df_clientes, clientes_nuevos[['id_cliente','idx']], on='idx')
df = df.reset_index(drop=True)
df['idx'] = df.index
df = df.merge(df_merged[['idx','id_cliente']], on='idx', how='left')

# ==============================
# 🔟 Insertar cliente_plan_info
# ==============================
df_stg = df[['id_cliente','id_periodo']].dropna().astype({'id_cliente':int,'id_periodo':int})
print(f"Insertando {len(df_stg)} registros en cliente_plan_info...")

try:
    df_stg.to_sql('cliente_plan_info', engine, if_exists='append', index=False)
    print(" Carga completa en cliente_plan_info.")
except SQLAlchemyError as e:
    print(f"Error al insertar en cliente_plan_info: {e}")
 
