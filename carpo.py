import pandas as pd
from sqlalchemy import create_engine
import glob
import os
import sys

# ==============================
# 1️⃣ Conexión a PostgreSQL
# ==============================
usuario = 'postgres'
contraseña = 'pasante'
host = 'localhost'
puerto = '5432'
base_datos = 'pospago'

connection_string = f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}'
engine = create_engine(connection_string)

# ==============================
# 2️⃣ Leer Excel (todas las hojas)
# ==============================
ruta_excel = r'C:\Users\pasante.ti2\Desktop\bases pospago\nuevo\base_2023.xlsx'
try:
    print("📥 Leyendo archivo Excel (todas las hojas)...")
    hojas = pd.read_excel(ruta_excel, sheet_name=None)
    df_list = []
    for nombre_hoja, df_hoja in hojas.items():
        df_hoja.columns = [col.lower().strip() for col in df_hoja.columns]
        df_list.append(df_hoja)
    df = pd.concat(df_list, ignore_index=True)
    print(f"✅ Total registros cargados de todas las hojas: {len(df)}")
except Exception as e:
    sys.exit(f"❌ Error leyendo Excel: {e}")

# ==============================
# 🔧 Normalizar columnas de período
# ==============================
df['año'] = df['año'].astype(str).str.strip()
df['mes'] = df['mes'].astype(str).str.strip().str.upper()
df['texto_extraido'] = df['texto_extraido'].astype(str).str.strip()

# Obtener id_anio
año = str(int(float(df['año'].dropna().unique()[0]))).strip()
anio_result = pd.read_sql(f"SELECT id_anio FROM anio WHERE valor = '{año}'", engine)
if anio_result.empty:
    raise ValueError(f"❌ Año '{año}' no encontrado en la tabla 'anio'.")

id_anio = anio_result.iloc[0]['id_anio']

# Crear períodos por cada mes único
periodos = []
meses_unicos = df['mes'].dropna()
meses_unicos = meses_unicos[meses_unicos.str.strip() != ''].unique()
texto_extraido = df['texto_extraido'].dropna().unique()[0]

for mes in meses_unicos:
    mes_result = pd.read_sql(f"SELECT id_mes FROM mes WHERE nombre_mes = '{mes}'", engine)

    if mes_result.empty:
        raise ValueError(f"❌ Mes '{mes}' no encontrado en la tabla 'mes'.")

    id_mes = mes_result.iloc[0]['id_mes']

    # Verificar existencia del período
    query = f"""
        SELECT id_periodo FROM periodo_carga
        WHERE id_anio = {id_anio} AND id_mes = {id_mes} AND texto_extraido = '{texto_extraido}'
    """
    existente = pd.read_sql(query, engine)

    if not existente.empty:
        id_periodo = existente.iloc[0]['id_periodo']
        print(f"ℹ️ Período ya existente: {mes} {año} → id_periodo = {id_periodo}")
    else:
        df_periodo = pd.DataFrame([{
            'id_anio': id_anio,
            'id_mes': id_mes,
            'texto_extraido': texto_extraido
        }])
        df_periodo.to_sql('periodo_carga', engine, if_exists='append', index=False)
        id_periodo = pd.read_sql('SELECT MAX(id_periodo) AS id FROM periodo_carga', engine).iloc[0]['id']
        print(f"🆕 Nuevo período insertado: {mes} {año} → id_periodo = {id_periodo}")

    periodos.append({'mes': mes, 'id_periodo': id_periodo})

# Asignar id_periodo a cada fila
periodo_map = pd.DataFrame(periodos)
df = df.merge(periodo_map, on='mes', how='left')

# ==============================
# 3️⃣ Normalizar columnas clave
# ==============================
cols_clave = [
    'identificacion', 'tipo_identificacion', 'provincia', 'ciudad',
    'institucion_financiera', 'desc_forma_pago', 'id_plan', 'id_ciclo', 'id_subproducto'
]

for col in cols_clave:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.upper()

# ==============================
# 4️⃣ Función para insertar en tablas auxiliares
# ==============================
def insertar_auxiliar(df_origen, columna, tabla_sql, columna_sql):
    print(f"\n📋 Tabla: {tabla_sql}")
    df_aux = pd.DataFrame({columna_sql: df_origen[columna].dropna().drop_duplicates()})
    df_aux[columna_sql] = df_aux[columna_sql].astype(str).str.strip().str.upper()

    existentes = pd.read_sql(f'SELECT {columna_sql} FROM {tabla_sql}', engine)
    existentes[columna_sql] = existentes[columna_sql].astype(str).str.strip().str.upper()

    df_aux = df_aux[~df_aux[columna_sql].isin(existentes[columna_sql])]
    print(f"🆕 Nuevos a insertar: {len(df_aux)}")

    if not df_aux.empty:
        try:
            df_aux.to_sql(tabla_sql, engine, if_exists='append', index=False)
            print(f"✅ Insertados en '{tabla_sql}': {len(df_aux)}")
        except Exception as e:
            print(f"❌ Error al insertar en '{tabla_sql}': {e}")
    else:
        print(f"⚠️ Todos los registros ya existen en '{tabla_sql}'.")

# ==============================
# 5️⃣ Insertar tablas auxiliares
# ==============================
insertar_auxiliar(df, 'tipo_identificacion', 'tipo_identificacion', 'nombre_tipo')
insertar_auxiliar(df, 'provincia', 'provincia', 'nombre_provincia')
insertar_auxiliar(df, 'institucion_financiera', 'institucion_financiera', 'nombre_institucion')
insertar_auxiliar(df, 'desc_forma_pago', 'forma_pago', 'desc_forma_pago')
insertar_auxiliar(df, 'id_subproducto', 'subproducto', 'id_subproducto')
insertar_auxiliar(df, 'id_ciclo', 'ciclo', 'id_ciclo')

# ==============================
# 6️⃣ Insertar ciudad con mapeo de provincia
# ==============================
print("\n📋 Tabla: ciudad")
prov_map = pd.read_sql('SELECT id_provincia, nombre_provincia FROM provincia', engine)
prov_map['nombre_provincia'] = prov_map['nombre_provincia'].astype(str).str.strip().str.upper()

df_ciudad = df[['ciudad', 'provincia']].dropna().drop_duplicates()
df_ciudad = df_ciudad.merge(prov_map, left_on='provincia', right_on='nombre_provincia')
df_ciudad = df_ciudad.rename(columns={'ciudad': 'nombre_ciudad'})

existentes = pd.read_sql('SELECT nombre_ciudad FROM ciudad', engine)
df_ciudad = df_ciudad[~df_ciudad['nombre_ciudad'].isin(existentes['nombre_ciudad'])]
print(f"🆕 Nuevos a insertar: {len(df_ciudad)}")

if not df_ciudad.empty:
    df_ciudad[['nombre_ciudad', 'id_provincia']].to_sql('ciudad', engine, if_exists='append', index=False)
    print(f"✅ Insertados en 'ciudad': {len(df_ciudad)}")
else:
    print("⚠️ Todos los registros ya existen en 'ciudad'.")

# ==============================
# 7️⃣ Insertar planes
# ==============================
df_plan = df[['id_plan', 'descripcion_plan']].dropna().drop_duplicates()
df_plan['id_plan'] = df_plan['id_plan'].astype(str).str.strip().str.upper()
df_plan['descripcion_plan'] = df_plan['descripcion_plan'].astype(str).str.strip()

existentes = pd.read_sql('SELECT id_plan FROM plan', engine)
existentes['id_plan'] = existentes['id_plan'].astype(str).str.strip().str.upper()

df_plan_filtrado = df_plan[~df_plan['id_plan'].isin(existentes['id_plan'])]
print(f"🆕 Nuevos a insertar: {len(df_plan_filtrado)}")

if not df_plan_filtrado.empty:
    try:
        df_plan_filtrado.to_sql('plan', engine, if_exists='append', index=False)
        print(f"✅ Insertados en 'plan': {len(df_plan_filtrado)}")
    except Exception as e:
        print(f"❌ Error al insertar en 'plan': {e}")
else:
    print("⚠️ Todos los registros ya existen en 'plan'.")

# ==============================
# 8️⃣ Mapear auxiliares para cliente y cliente_plan_info
# ==============================
def cargar_tabla_auxiliar(query, columna_clave):
    tabla = pd.read_sql(query, engine)
    tabla[columna_clave] = tabla[columna_clave].astype(str).str.strip().str.upper()
    return tabla.drop_duplicates(subset=[columna_clave])

tipo_map = cargar_tabla_auxiliar('SELECT id_tipo_ident, nombre_tipo FROM tipo_identificacion', 'nombre_tipo')
prov_map = cargar_tabla_auxiliar('SELECT id_provincia, nombre_provincia FROM provincia', 'nombre_provincia')
ciudad_map = cargar_tabla_auxiliar('SELECT id_ciudad, nombre_ciudad FROM ciudad', 'nombre_ciudad')
inst_map = cargar_tabla_auxiliar('SELECT id_institucion, nombre_institucion FROM institucion_financiera', 'nombre_institucion')
pago_map = cargar_tabla_auxiliar('SELECT id_forma_pago, desc_forma_pago FROM forma_pago', 'desc_forma_pago')

def merge_con_log(df, tabla_aux, columna_df, columna_aux, nombre_tabla):
    antes = len(df)
    df = df.merge(tabla_aux, left_on=columna_df, right_on=columna_aux, how='left')
    print(f"🔄 Merge con {nombre_tabla}: antes={antes}, después={len(df)}")
    return df

df = merge_con_log(df, tipo_map, 'tipo_identificacion', 'nombre_tipo', 'tipo_identificacion')
df = merge_con_log(df, prov_map, 'provincia', 'nombre_provincia', 'provincia')
df = merge_con_log(df, ciudad_map, 'ciudad', 'nombre_ciudad', 'ciudad')
df = merge_con_log(df, inst_map, 'institucion_financiera', 'nombre_institucion', 'institucion_financiera')
df = merge_con_log(df, pago_map, 'desc_forma_pago', 'desc_forma_pago', 'forma_pago')

# ==============================
# 9️⃣ Insertar clientes
# ==============================
df_cliente = df[['identificacion', 'nombre_completo', 'celular', 'fecha_alta']].copy()
df_cliente['celular'] = df_cliente['celular'].astype(str).str.strip().apply(lambda x: x if x.startswith('0') else '0' + x)
df_cliente['fecha_alta'] = pd.to_datetime(df_cliente['fecha_alta'], errors='coerce', dayfirst=True)
df_cliente['id_tipo_ident'] = df['id_tipo_ident']
df_cliente['id_provincia'] = df['id_provincia']
df_cliente['id_ciudad'] = df['id_ciudad']

print(f"🔢 Registros a insertar en cliente: {len(df_cliente)}")
df_cliente.to_sql('cliente', engine, if_exists='append', index=False)
print(f"✅ Insertados en 'cliente': {len(df_cliente)}")

# ==============================
# 🔟 Mapear id_cliente
# ==============================
def normalizar_celular(c):
    c = str(c).strip()
    if c.startswith('0'):
        return c
    elif len(c) == 9:
        return '0' + c
    return c

cliente_map = pd.read_sql('SELECT id_cliente, identificacion, celular FROM cliente', engine)
cliente_map['identificacion'] = cliente_map['identificacion'].astype(str).str.strip().str.upper()
cliente_map['celular'] = cliente_map['celular'].apply(normalizar_celular)
cliente_map = cliente_map.drop_duplicates(subset=['identificacion', 'celular'], keep='last')

df['identificacion'] = df['identificacion'].astype(str).str.strip().str.upper()
df['celular'] = df['celular'].apply(normalizar_celular)

df = df.merge(cliente_map, on=['identificacion', 'celular'], how='left')
print(f"✅ Registros con id_cliente asignado: {df['id_cliente'].notnull().sum()}")
print(f"❌ Registros sin id_cliente asignado: {df['id_cliente'].isnull().sum()}")

# ==============================
# 11️⃣ Verificar campos nulos antes de insertar en cliente_plan_info
# ==============================
print("\n📊 Verificando campos nulos antes de insertar en cliente_plan_info:")
for col in ['id_cliente', 'id_plan', 'id_ciclo', 'id_forma_pago', 'id_institucion']:
    print(f"❌ Registros sin {col}: {df[col].isnull().sum()}")

# ==============================
# 12️⃣ Insertar cliente_plan_info
# ==============================
df_plan_info = df[[
    'id_cliente', 'id_plan', 'id_subproducto', 'id_ciclo', 'id_forma_pago',
    'id_institucion', 'tb', 'categoria1', 'id_periodo'
]].copy()

df_plan_info = df_plan_info.dropna(subset=[
    'id_cliente', 'id_plan', 'id_subproducto', 'id_ciclo',
    'id_forma_pago', 'id_institucion', 'id_periodo'
])

df_plan_info['id_cliente'] = df_plan_info['id_cliente'].astype(int)
df_plan_info['id_plan'] = df_plan_info['id_plan'].astype(str).str.strip().str.upper()
df_plan_info['id_subproducto'] = df_plan_info['id_subproducto'].astype(str).str.strip().str.upper()
df_plan_info['id_ciclo'] = df_plan_info['id_ciclo'].astype(int)
df_plan_info['id_forma_pago'] = df_plan_info['id_forma_pago'].astype(int)
df_plan_info['id_institucion'] = df_plan_info['id_institucion'].astype(int)
df_plan_info['id_periodo'] = df_plan_info['id_periodo'].astype(int)
df_plan_info['tb'] = pd.to_numeric(df_plan_info['tb'], errors='coerce')
df_plan_info['categoria1'] = df_plan_info['categoria1'].astype(str).str.strip()
df_plan_info = df_plan_info.dropna(subset=['tb'])

# Insertar subproductos faltantes
subproductos_existentes = pd.read_sql('SELECT id_subproducto FROM subproducto', engine)
subproductos_existentes['id_subproducto'] = subproductos_existentes['id_subproducto'].astype(str).str.strip().str.upper()

df_subproducto = pd.DataFrame({'id_subproducto': df_plan_info['id_subproducto'].dropna().unique()})
df_subproducto['id_subproducto'] = df_subproducto['id_subproducto'].astype(str).str.strip().str.upper()

df_subproducto_nuevos = df_subproducto[~df_subproducto['id_subproducto'].isin(subproductos_existentes['id_subproducto'])]

if not df_subproducto_nuevos.empty:
    df_subproducto_nuevos.to_sql('subproducto', engine, if_exists='append', index=False)
    print(f"✅ Insertados en 'subproducto': {len(df_subproducto_nuevos)}")
else:
    print("⚠️ Todos los subproductos ya existen.")

# Insertar cliente_plan_info
print(f"🔢 Registros válidos a insertar en cliente_plan_info: {len(df_plan_info)}")
if not df_plan_info.empty:
    try:
        df_plan_info.to_sql('cliente_plan_info', engine, if_exists='append', index=False)
        print(f"✅ Insertados en 'cliente_plan_info': {len(df_plan_info)}")
    except Exception as e:
        print(f"❌ Error al insertar en 'cliente_plan_info': {e}")
else:
    print("⚠️ No hay registros válidos para insertar en 'cliente_plan_info'.")
