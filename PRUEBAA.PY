import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ==============================
# CONFIGURACIÓN
# ==============================
usuario = 'postgres'
contraseña = quote_plus('pasante')
host = 'localhost'
puerto = '5432'

base_origen = 'base_pyme'  # 'base_pyme', 'base_pospago', 'base_prepago'
base_destino = 'BAS'
tabla_destino = 'cliente_consolidado'

# ==============================
# MAPA DE IDS FIJOS PARA ORIGEN
# ==============================
map_base_to_id = {
    'base_pyme': 1,     
    'pospago': 2,  
    'prepago': 3   
}

map_base_to_nombre = {
    'base_pyme': 'PYME',
    'pospago': 'POSPAGO',
    'prepago': 'PREPAGO'
}

id_origen = map_base_to_id.get(base_origen.lower())
nombre_origen = map_base_to_nombre.get(base_origen.lower(), 'DESCONOCIDO')

if id_origen is None:
    raise ValueError(f"No existe ID definido para la base {base_origen}")

# ==============================
# CONEXIÓN A BASE DESTINO
# ==============================
engine_destino = create_engine(
    f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_destino}',
    connect_args={"options": "-c client_encoding=UTF8"}
)

# ==============================
# ASEGURAR QUE EXISTAN LOS ORÍGENES EN LA TABLA
# ==============================
with engine_destino.begin() as conn:  # <- begin maneja commit automáticamente
    for base, nombre in map_base_to_nombre.items():
        conn.execute(
            text("""
                INSERT INTO origen (id_origen, nombre_origen)
                VALUES (:id_origen, :nombre_origen)
                ON CONFLICT (id_origen) DO NOTHING;
            """),
            {"id_origen": map_base_to_id[base], "nombre_origen": nombre}
        )

# ==============================
# CONEXIÓN A BASE ORIGEN
# ==============================
engine_origen = create_engine(
    f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_origen}',
    connect_args={"options": "-c client_encoding=UTF8"}
)

# ==============================
# CONSULTA CLIENTES CON AÑO Y MES
# ==============================
query = """
    SELECT 
        c.celular,
        c.identificacion,
        c.nombre_completo,
        pc.texto_extraido,
        pc.id_anio,
        pc.id_mes,
        '' AS proveedor
    FROM cliente c
    JOIN cliente_plan_info cpi ON c.id_cliente = cpi.id_cliente
    JOIN periodo_carga pc ON cpi.id_periodo = pc.id_periodo
"""

df_origen = pd.read_sql(query, engine_origen)

# ==============================
# LIMPIEZA Y PREPARACIÓN
# ==============================
for col in ['celular', 'identificacion', 'nombre_completo', 'texto_extraido']:
    df_origen[col] = df_origen[col].astype(str).str.strip()

df_origen['id_origen'] = id_origen
df_origen['proveedor'] = ''  # Mantener vacío

# ==============================
# FILTRAR SOLO NUEVOS REGISTROS POR AÑO Y MES
# ==============================
with engine_destino.connect() as conn:
    df_existentes = pd.read_sql(
        text("""
            SELECT celular, identificacion, id_origen, id_anio, id_mes
            FROM cliente_consolidado
            WHERE id_origen = :id_origen
        """),
        conn,
        params={"id_origen": id_origen}
    )

df_nuevos = df_origen.merge(
    df_existentes,
    on=['celular','identificacion','id_origen','id_anio','id_mes'],
    how='left',
    indicator=True
).query('_merge == "left_only"').drop(columns=['_merge'])

# ==============================
# INSERCIÓN SOLO DE NUEVOS
# ==============================
if len(df_nuevos) > 0:
    with engine_destino.begin() as conn:  # Commit automático
        df_nuevos.to_sql(tabla_destino, conn, if_exists='append', index=False)
    print(f"✅ {len(df_nuevos)} nuevos registros insertados en {tabla_destino} desde {nombre_origen}")
else:
    print(f"ℹ️ No hay registros nuevos para insertar desde {nombre_origen}")





