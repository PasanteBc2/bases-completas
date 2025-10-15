import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# ==============================
# Configuración de la base de datos
# ==============================
usuario = 'postgres'
contraseña = 'pasante'
host = 'localhost'
puerto = '5432'
base_datos = 'prepago'

engine = create_engine(f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}')

# ==============================
# Lectura del Excel (ambas hojas)
# ==============================
print("📥 Leyendo archivo Excel...")

hojas = pd.read_excel(
    'C:\\Users\\pasante.ti2\\Desktop\\bases prepago\\base_pre_2025.xlsx',
    sheet_name=['Hoja1']
)

# Convertir hojas a DataFrames y unificarlas
df_hoja1 = hojas['Hoja1']

# Normalizar nombres de columnas
df_hoja1.columns = [col.lower().strip() for col in df_hoja1.columns]

# Unir ambas hojas en un solo DataFrame
df = pd.concat([df_hoja1], ignore_index=True)
print(f"✅ Total registros cargados: {len(df)}")

# ==============================
# Normalización de columnas y limpieza
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

# 🔧 Limpieza de identificaciones con '.0'
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

# 🔧 Normalización de celulares
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
# Cargar tablas de referencia desde la DB
# ==============================
df_anio = pd.read_sql('SELECT * FROM anio', engine)
df_mes = pd.read_sql('SELECT * FROM mes', engine)

df = df.merge(df_anio, left_on='anio', right_on='valor', how='left')
df = df.merge(df_mes, left_on='mes', right_on='nombre_mes', how='left')

if df['id_anio'].isnull().any() or df['id_mes'].isnull().any():
    print("❌ Error: Algunos registros tienen año o mes inválido")
    exit()

# ==============================
# Insertar nuevos periodos
# ==============================
df_periodos = df[['id_anio', 'id_mes', 'texto_extraido']].drop_duplicates()
periodos_existentes = pd.read_sql('SELECT id_anio, id_mes, texto_extraido FROM periodo_carga', engine)

df_nuevos_periodos = df_periodos.merge(
    periodos_existentes,
    on=['id_anio', 'id_mes', 'texto_extraido'],
    how='left',
    indicator=True
).query("_merge == 'left_only'").drop(columns=['_merge'])

if not df_nuevos_periodos.empty:
    print(f"🆕 Insertando {len(df_nuevos_periodos)} nuevos períodos...")
    df_nuevos_periodos.to_sql('periodo_carga', engine, if_exists='append', index=False)
else:
    print("ℹ️ No hay nuevos períodos.")

# Refrescar tabla periodo_carga con IDs
df_periodos_actualizados = pd.read_sql(
    'SELECT id_periodo, id_anio, id_mes, texto_extraido FROM periodo_carga', engine
)
df = df.merge(df_periodos_actualizados, on=['id_anio', 'id_mes', 'texto_extraido'], how='left')

# ==============================
# Preparar DataFrame clientes
# ==============================
df_clientes = df[['identificacion', 'celular', 'monto_recarga', 'nombre_completo']].copy()

print(f"📋 Insertando {len(df_clientes)} registros en tabla cliente...")

try:
    df_clientes.to_sql('cliente', engine, if_exists='append', index=False)
except SQLAlchemyError as e:
    print(f"❌ Error insertando clientes: {e}")
    exit()

# ==============================
# Relacionar filas con id_cliente
# ==============================
clientes_totales = pd.read_sql('SELECT id_cliente, identificacion, celular, nombre_completo FROM cliente', engine)
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
# Preparar info_cliente_stg
# ==============================
df_stg = df[['id_cliente', 'id_periodo']].dropna().astype({'id_cliente': int, 'id_periodo': int})

print(f"📥 Insertando {len(df_stg)} registros en info_cliente_stg...")

try:
    df_stg.to_sql('info_cliente_stg', engine, if_exists='append', index=False)
    print("✅ Carga completa.")
except SQLAlchemyError as e:
    print(f"❌ Error al insertar en info_cliente_stg: {e}")
