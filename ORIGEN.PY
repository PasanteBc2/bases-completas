import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text
# ==============================
# 1️⃣ Configuración de logs
# ==============================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==============================
# 2️⃣ Credenciales / host
# ==============================
usuario = "analista"
contraseña = "2025Anal1st@"
host = "192.168.10.37"
puerto = 5432

# ==============================
# 3️⃣ Mapas de bases
# ==============================
map_nombres_bases = {
    'base_pyme': 'BcorpPymePrueba',
    'pospago':   'BcorpPostPrueba',
    'prepago':   'BcorpPrePrueba'
}

base_origen = 'prepago'      # base_pyme | pospago | prepago
base_destino = 'BAS'
tabla_destino = 'cliente_consolidado'

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
# 4️⃣ Crear engine usando URL.create
# ==============================
def create_engine_for(database_name: str):
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=usuario,
        password=contraseña,
        host=host,
        port=puerto,
        database=database_name,
    )
    try:
        eng = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_timeout=60,
        )
        with eng.connect() as conn:
            logging.info(f"✅ Conexión OK → DB={database_name}")
        return eng
    except OperationalError as e:
        logging.exception(f"❌ Error de conexión → DB={database_name}")
        raise SystemExit(e)

# Crear engines
engine_destino = create_engine_for(base_destino)

nombre_db_origen = map_nombres_bases.get(base_origen)
if not nombre_db_origen:
    raise ValueError(f"No hay mapeo para base origen: {base_origen}")

engine_origen = create_engine_for(nombre_db_origen)

# ==============================
# 5️⃣ Asegurar orígenes
# ==============================
with engine_destino.begin() as conn:
    for alias, nombre in map_base_to_nombre.items():
        conn.execute(
            text("""
                INSERT INTO origen (id_origen, nombre_origen)
                VALUES (:id_origen, :nombre_origen)
                ON CONFLICT (id_origen) DO NOTHING;
            """),
            {"id_origen": map_base_to_id[alias], "nombre_origen": nombre}
        )

# ==============================
# 6️⃣ Consulta origen (CORREGIDA Y VALIDADA)
# ==============================
query = """
WITH ultimo_periodo AS (
    SELECT 
        cpi.id_cliente,
        cpi.id_periodo,
        ROW_NUMBER() OVER (
            PARTITION BY cpi.id_cliente 
            ORDER BY cpi.id_periodo DESC
        ) AS rn
    FROM cliente_plan_info cpi
)
SELECT 
    c.celular,
    c.identificacion,
    c.nombre_completo,
    pc.texto_extraido,
    pc.id_anio,
    pc.id_mes,
    pc.nombre_base,
    '' AS proveedor
FROM cliente c
LEFT JOIN ultimo_periodo up 
    ON c.id_cliente = up.id_cliente AND up.rn = 1
LEFT JOIN periodo_carga pc 
    ON up.id_periodo = pc.id_periodo;
"""

df_origen = pd.read_sql(query, engine_origen)

# ==============================
# 7️⃣ Añadir id_origen sin tocar columnas
# ==============================
df_origen['id_origen'] = id_origen

# ==============================
# 8️⃣ Insertar todos los datos tal cual
# ==============================
if len(df_origen) > 0:
    with engine_destino.begin() as conn:
        df_origen.drop(columns=['proveedor'], errors='ignore').to_sql(
            tabla_destino,
            conn,
            if_exists='append',
            index=False
        )
    print(f"✅ {len(df_origen)} registros insertados desde {nombre_origen}")
else:
    print(f"ℹ️ No hay registros para insertar desde {nombre_origen}")
