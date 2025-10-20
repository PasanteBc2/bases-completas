import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# ==============================
# 1️⃣ CONFIGURACIÓN GENERAL
# ==============================
usuario = 'postgres'
contraseña = quote_plus('pasante')  # Codifica caracteres especiales
host = 'localhost'
puerto = '5432'

# 🔁 Solo cambia esta variable según la base que quieras leer
# Opciones: 'prepago' o 'pospago'
base_origen = 'base_pyme'   # 👈 cambia aquí a 'pospago' cuando lo necesites

# Base destino en PgAdmin
base_destino = 'BASES'

# Tabla destino
tabla_destino = 'cliente_consolidado'

# ==============================
# 2️⃣ CONEXIÓN A LA BASE DE ORIGEN
# ==============================
engine_origen = create_engine(
    f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_origen}',
    connect_args={"options": "-c client_encoding=UTF8"}
)

# ==============================
# 3️⃣ CONSULTA AUTOMÁTICA
# ==============================

query = f"""
    SELECT 
        c.celular,
        c.identificacion,
        c.nombre_completo,
        pc.texto_extraido,
        'PYME' AS origen, 
        '' AS proveedor
    FROM cliente c
   JOIN cliente_plan_info cpi ON c.id_cliente = cpi.id_cliente
   JOIN periodo_carga pc ON cpi.id_periodo = pc.id_periodo
   WHERE cpi.id_periodo = 29
"""
#'PYME' AS origen, 
#WHERE cpi.id_periodo = 29
#UPPER('{base_origen}') AS origen,

df_origen = pd.read_sql(query, engine_origen)

# ==============================
# 4️⃣ LIMPIEZA DE DATOS
# ==============================
for col in ['celular', 'identificacion', 'nombre_completo', 'texto_extraido']:
    df_origen[col] = df_origen[col].astype(str).str.strip()

print(f"✅ Total registros de {base_origen.upper()}: {len(df_origen)}")
print(df_origen.head())

# ==============================
# 5️⃣ CONEXIÓN A LA BASE DE DESTINO
# ==============================
engine_destino = create_engine(
    f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_destino}',
    connect_args={"options": "-c client_encoding=UTF8"}
)

# ==============================
# 6️⃣ INSERCIÓN AUTOMÁTICA EN PgAdmin
# ==============================
df_origen.to_sql(tabla_destino, engine_destino, if_exists='append', index=False)
print(f"✅ Registros insertados en {tabla_destino} desde {base_origen.upper()}: {len(df_origen)}")
