import pandas as pd  # manipular y analizar datos en formato de tablas (DataFrames)
from sqlalchemy import create_engine  # Crea la conexión entre Python y la base de datos 
from sqlalchemy.exc import SQLAlchemyError, OperationalError  # Manejar errores en la conexión o consultas SQL
import sys  # Interactuacon el sistema (por ejemplo, cerrar el programa o leer argumentos externos)
import tkinter as tk  # Interfaz gráfica para seleccionar archivos
from tkinter import filedialog # Diálogo para seleccionar archivos

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
    sys.exit(f"❌ No se pudo conectar a la base de datos: {e}")
except Exception as e:
    sys.exit(f"⚠️ Error inesperado al conectar a la base de datos: {e}")

# ==============================
# 2️⃣ Leer todas las hojas del Excel
# ==============================
# Ocultar ventana principal de Tkinter
root = tk.Tk()
root.withdraw()

# Seleccionar archivo Excel manualmente
ruta_excel = filedialog.askopenfilename(
    title="Selecciona el archivo Excel",
    filetypes=[("Archivos Excel", "*.xlsx *.xls")]
)

if not ruta_excel:
    sys.exit("❌ No se seleccionó ningún archivo. Ejecución cancelada.")

try: 
    print(f"📥 Leyendo archivo Excel seleccionado:\n{ruta_excel}")
    hojas = pd.read_excel(ruta_excel, sheet_name=None)

    df_list = []
    for nombre_hoja, df_hoja in hojas.items():
        df_hoja.columns = [col.lower().strip() for col in df_hoja.columns]
        df_list.append(df_hoja)
    df = pd.concat(df_list, ignore_index=True)
    print(f"✅ Total registros cargados de todas las hojas: {len(df)}")
except Exception as e:
    sys.exit(f"❌ Error leyendo Excel: {e}") 
    df_list = []
    for nombre_hoja, df_hoja in hojas.items():
        df_hoja.columns = [col.lower().strip() for col in df_hoja.columns]
        df_list.append(df_hoja)
    df = pd.concat(df_list, ignore_index=True)
    print(f"✅ Total registros cargados de todas las hojas: {len(df)}")
except Exception as e:
    sys.exit(f"❌ Error leyendo Excel: {e}")
 
# ==============================
# 3️⃣ Normalización y limpieza
# ==============================
df.rename(columns={'año': 'anio'}, inplace=True)
df = df.fillna('')
df['anio'] = df['anio'].astype(str).str.strip()
df['mes'] = df['mes'].astype(str).str.strip().str.upper()
df['texto_extraido'] = df['texto_extraido'].astype(str).str.strip()
df['nombre_completo'] = df['nombre_completo'].astype(str).str.strip()
df['monto_recarga'] = df['monto_recarga'].astype(str).str.strip()
df['identificacion'] = df['identificacion'].astype(str).str.strip()
df['celular'] = df['celular'].astype(str).str.strip()

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
corregidos_identificacion = df['identificacion'].str.endswith('.0').sum()
print(f"🔍 Identificaciones corregidas: {corregidos_identificacion}")

def normalizar_celular(valor):
    valor = str(valor).strip()
    if valor == '':
        return ''
    try:
        return str(int(float(valor))).zfill(10)
    except ValueError:
        return valor

df['celular'] = df['celular'].apply(normalizar_celular)
celulares_corregidos = df['celular'].apply(lambda x: len(x) < 10 or not x.isdigit()).sum()
print(f"📱 Celulares potencialmente inconsistentes: {celulares_corregidos}")

# ==============================
# 4️⃣ Funciones de SQL con transacción y excepciones
# ==============================
def ejecutar_sql(sql, params=None):
    try:
        with engine.begin() as conn:
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)
        return True
    except SQLAlchemyError as e:
        print(f"⚠️ Error ejecutando SQL: {e}")
        return False

def leer_sql(query):
    try:
        return pd.read_sql(query, engine)
    except SQLAlchemyError as e:
        print(f"⚠️ Error leyendo SQL: {e}")
        return pd.DataFrame()

# ==============================
# 5️⃣ Cargar tablas de referencia
# ==============================
df_anio = leer_sql('SELECT * FROM anio')
df_mes = leer_sql('SELECT * FROM mes')

df = df.merge(df_anio, left_on='anio', right_on='valor', how='left')
df = df.merge(df_mes, left_on='mes', right_on='nombre_mes', how='left')

if df['id_anio'].isnull().any() or df['id_mes'].isnull().any():
    sys.exit("❌ Error: Algunos registros tienen año o mes inválido")

# ==============================
# 6️⃣ Insertar nuevos periodos
# ==============================
df_periodos = df[['id_anio', 'id_mes', 'texto_extraido']].drop_duplicates().copy()

# 👉 Agregar el campo nombre_base (en minúsculas con prefijo b_ppa_)
df_periodos['nombre_base'] = 'b_ppa_' + df_periodos['texto_extraido'].str.lower()

periodos_existentes = leer_sql('SELECT id_anio, id_mes, texto_extraido FROM periodo_carga')

df_nuevos_periodos = df_periodos.merge(
    periodos_existentes,
    on=['id_anio', 'id_mes', 'texto_extraido'],
    how='left',
    indicator=True
).query("_merge == 'left_only'").drop(columns=['_merge'])

if not df_nuevos_periodos.empty:
    print(f"🆕 Insertando {len(df_nuevos_periodos)} nuevos períodos...")
    try:
        df_nuevos_periodos.to_sql('periodo_carga', engine, if_exists='append', index=False)
    except SQLAlchemyError as e:
        print(f"❌ Error insertando nuevos periodos: {e}")
else:
    print("ℹ️ No hay nuevos períodos.")

df_periodos_actualizados = leer_sql(
    'SELECT id_periodo, id_anio, id_mes, texto_extraido FROM periodo_carga'
)
df = df.merge(df_periodos_actualizados, on=['id_anio', 'id_mes', 'texto_extraido'], how='left')

# ==============================
# 7️⃣ Insertar clientes
# ==============================
df_clientes = df[['identificacion', 'celular', 'monto_recarga', 'nombre_completo']].copy()
print(f"📋 Insertando {len(df_clientes)} registros en tabla cliente...")

try:
    df_clientes.to_sql('cliente', engine, if_exists='append', index=False)
except SQLAlchemyError as e:
    sys.exit(f"❌ Error insertando clientes: {e}")

# ==============================
# 8️⃣ Relacionar filas con id_cliente
# ==============================
clientes_totales = leer_sql('SELECT id_cliente, identificacion, celular, nombre_completo FROM cliente')
n = len(df_clientes)
clientes_nuevos = clientes_totales.tail(n).copy()

df_clientes = df_clientes.reset_index(drop=True)
clientes_nuevos = clientes_nuevos.reset_index(drop=True)
df_clientes['idx'] = df_clientes.index
clientes_nuevos['idx'] = clientes_nuevos.index

df_merged = pd.merge(df_clientes, clientes_nuevos[['id_cliente', 'idx']], on='idx')

df = df.reset_index(drop=True)
df['idx'] = df.index
df = df.merge(df_merged[['idx', 'id_cliente']], on='idx', how='left')

# ==============================
# 9️⃣ Insertar cliente_plan_info con transacción
# ==============================
df_stg = df[['id_cliente', 'id_periodo']].dropna().astype({'id_cliente': int, 'id_periodo': int})
print(f"📥 Insertando {len(df_stg)} registros en cliente_plan_info...")

try:
    df_stg.to_sql('cliente_plan_info', engine, if_exists='append', index=False)
    print("✅ Carga completa en cliente_plan_info.")
except SQLAlchemyError as e:
    print(f"❌ Error al insertar en cliente_plan_info: {e}")
