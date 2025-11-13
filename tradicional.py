import os
import glob
import pandas as pd
from sqlalchemy import create_engine

# ==============================
# 1️⃣ Configuración PostgreSQL
# ==============================
usuario = 'postgres'
contraseña = 'pasante'
host = 'localhost'
puerto = '5432'
base_datos = 'tradicional'

engine = create_engine(f'postgresql+psycopg2://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}')

# ==============================
# 2️⃣ Carpeta principal
# ==============================
carpeta_principal = r'C:\Users\pasante.ti2\Documents\MOVISTAR\TRADICIONAL'
archivos_excel = glob.glob(os.path.join(carpeta_principal, '**', '*.xlsx'), recursive=True)

archivos_excel = [f for f in archivos_excel if not os.path.basename(f).startswith('~$')]
print(f"Se encontraron {len(archivos_excel)} archivos Excel válidos")

# ==============================
# 3️⃣ Columnas necesarias
# ==============================
columnas_necesarias = [
    'identificacion', 'nombre_completo', 'celular',
    'provincia', 'operadora_destino', 'deuda_movistar'
]

# ==============================
# 4️⃣ Función para normalizar texto
# ==============================
def normalizar_string(s):
    try:
        if pd.isna(s):
            return ""
        return str(s).strip().upper()
    except Exception:
        return str(s)

# ==============================
# 5️⃣ Leer y procesar archivos
# ==============================
lista_dfs = []
for archivo in archivos_excel:
    print(f"Leyendo archivo: {archivo}")
    try:
        df = pd.read_excel(archivo)
        df.columns = df.columns.str.strip().str.lower()
        lista_dfs.append(df)
    except Exception as e:
        print(f"Error leyendo {archivo}: {e}")

if not lista_dfs:
    raise ValueError("No se encontraron datos válidos en los archivos Excel.")

df_total = pd.concat(lista_dfs, ignore_index=True)
print("Columnas disponibles:", df_total.columns.tolist())

# ==============================
# 6️⃣ Asegurar columna provincia
# ==============================
if 'provincia' not in df_total.columns:
    df_total['provincia'] = "NO REGISTRA"

# ==============================
# 7️⃣ Filtrar columnas necesarias
# ==============================
columnas_existentes = [c for c in columnas_necesarias if c in df_total.columns]
df_total = df_total[columnas_existentes]

for col in ['nombre_completo', 'provincia', 'operadora_destino']:
    if col in df_total.columns:
        df_total[col] = df_total[col].map(normalizar_string)

# ==============================
# 8️⃣ Cargar provincias desde PostgreSQL
# ==============================
df_provincias_db = pd.read_sql("SELECT * FROM provincia", engine)

# Vincular id_provincia usando nombre
df_total = df_total.merge(df_provincias_db, left_on='provincia', right_on='nombre_provincia', how='left')

# ==============================
# 9️⃣ Preparar clientes
# ==============================
df_clientes = df_total.copy()
df_clientes['id_cliente'] = range(1, len(df_clientes)+1)
df_clientes = df_clientes[['id_cliente', 'identificacion', 'nombre_completo', 'celular',
                           'id_provincia', 'operadora_destino', 'deuda_movistar']]

# ==============================
# 🔟 Crear periodos simulados
# ==============================
df_total['periodo_texto'] = df_total.index.to_series().apply(lambda x: f"PERIODO_{x+1}")
df_periodos = df_total[['periodo_texto']].drop_duplicates().reset_index(drop=True)
df_periodos['id_periodo'] = range(1, len(df_periodos)+1)

# ==============================
# 1️⃣1️⃣ Cliente plan info
# ==============================
df_cliente_plan_info = df_total.merge(
    df_clientes[['id_cliente', 'identificacion']],
    on='identificacion', how='left', suffixes=('', '_cliente')
)
df_cliente_plan_info = df_cliente_plan_info.merge(df_periodos, on='periodo_texto', how='left')
df_cliente_plan_info['id_cliente_plan_info'] = range(1, len(df_cliente_plan_info)+1)
df_cliente_plan_info = df_cliente_plan_info[['id_cliente_plan_info', 'id_cliente', 'id_periodo']]

# ==============================
# 🧹 Limpiar columnas duplicadas
# ==============================
def limpiar_columnas(df):
    df.columns = df.columns.str.replace(r'__\d+$', '', regex=True)
    df = df.loc[:, ~df.columns.duplicated()]
    return df

df_clientes = limpiar_columnas(df_clientes)
df_periodos = limpiar_columnas(df_periodos)
df_cliente_plan_info = limpiar_columnas(df_cliente_plan_info)

# ==============================
# 1️⃣2️⃣ Cargar a PostgreSQL
# ==============================
df_clientes.to_sql('cliente', engine, if_exists='append', index=False)
df_periodos.to_sql('periodo_carga', engine, if_exists='append', index=False)
df_cliente_plan_info.to_sql('cliente_plan_info', engine, if_exists='append', index=False)

print("✅ Carga completada correctamente en PostgreSQL.")
