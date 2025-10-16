import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from datetime import datetime
import os

# ==============================
# 1️⃣ Conexión a PostgreSQL
# ==============================
usuario = 'postgres'
contraseña = 'pasante'
host = 'localhost'
puerto = '5432'
base_datos = 'pospago'

connection_string = f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}'
try:
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Conexión a PostgreSQL exitosa.")
except OperationalError as e:
    raise ConnectionError(f"❌ No se pudo conectar a PostgreSQL: {e}")

# ==============================
# 2️⃣ Leer Excel
# ==============================
try:
    print("📥 Leyendo archivo Excel...")
    ruta_excel = r'C:\Users\pasante.ti2\Desktop\bases pospago\nuevo\base_2025.xlsx'
    df = pd.read_excel(ruta_excel, sheet_name='Hoja1')
    df.columns = [col.lower() for col in df.columns]
    print(f"✅ Registros cargados desde Excel: {len(df)}")
except FileNotFoundError:
    raise FileNotFoundError(f"❌ No se encontró el archivo: {ruta_excel}")
except Exception as e:
    raise RuntimeError(f"❌ Error al leer Excel: {e}")

# ==============================
# 3️⃣ Normalizar columnas de período
# ==============================
try:
    df['año'] = df['año'].astype(str).str.strip()
    df['mes'] = df['mes'].astype(str).str.strip().str.upper()
    df['texto_extraido'] = df['texto_extraido'].astype(str).str.strip()

    año = str(int(float(df['año'].dropna().unique()[0]))).strip()
    anio_result = pd.read_sql(text("SELECT id_anio FROM anio WHERE valor = :año"), engine, params={"año": año})
    if anio_result.empty:
        raise ValueError(f"❌ Año '{año}' no encontrado en la tabla 'anio'.")
    id_anio = anio_result.iloc[0]['id_anio']

    periodos = []
    meses_unicos = df['mes'].dropna().unique()
    texto_extraido = df['texto_extraido'].dropna().unique()[0]

    for mes in meses_unicos:
        mes_result = pd.read_sql(text("SELECT id_mes FROM mes WHERE nombre_mes = :mes"), engine, params={"mes": mes})
        if mes_result.empty:
            raise ValueError(f"❌ Mes '{mes}' no encontrado en la tabla 'mes'.")
        id_mes = mes_result.iloc[0]['id_mes']

        query = text("""
            SELECT id_periodo FROM periodo_carga
            WHERE id_anio = :id_anio AND id_mes = :id_mes AND texto_extraido = :texto_extraido
        """)
        existente = pd.read_sql(query, engine, params={"id_anio": id_anio, "id_mes": id_mes, "texto_extraido": texto_extraido})

        if not existente.empty:
            id_periodo = existente.iloc[0]['id_periodo']
            print(f"ℹ️ Período ya existente: {mes} {año} → id_periodo = {id_periodo}")
        else:
            df_periodo = pd.DataFrame([{'id_anio': id_anio, 'id_mes': id_mes, 'texto_extraido': texto_extraido}])
            with engine.begin() as conn:
                df_periodo.to_sql('periodo_carga', conn, if_exists='append', index=False)
            id_periodo = pd.read_sql(text("SELECT MAX(id_periodo) AS id FROM periodo_carga"), engine).iloc[0]['id']
            print(f"🆕 Nuevo período insertado: {mes} {año} → id_periodo = {id_periodo}")

        periodos.append({'mes': mes, 'id_periodo': id_periodo})

    periodo_map = pd.DataFrame(periodos)
    df = df.merge(periodo_map, on='mes', how='left')
except Exception as e:
    raise RuntimeError(f"❌ Error procesando períodos: {e}")

# ==============================
# 4️⃣ Normalizar columnas clave
# ==============================
cols_clave = ['identificacion', 'tipo_identificacion', 'provincia', 'ciudad',
              'institucion_financiera', 'desc_forma_pago', 'id_plan', 'id_ciclo', 'id_subproducto']

for col in cols_clave:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.upper()

# ==============================
# 5️⃣ Función insertar auxiliar
# ==============================
def insertar_auxiliar(df_origen, columna, tabla_sql, columna_sql):
    try:
        print(f"\n📋 Tabla: {tabla_sql}")
        df_aux = pd.DataFrame({columna_sql: df_origen[columna].dropna().drop_duplicates()})
        df_aux[columna_sql] = df_aux[columna_sql].astype(str).str.strip().str.upper()

        existentes = pd.read_sql(text(f"SELECT {columna_sql} FROM {tabla_sql}"), engine)
        existentes[columna_sql] = existentes[columna_sql].astype(str).str.strip().str.upper()

        df_aux = df_aux[~df_aux[columna_sql].isin(existentes[columna_sql])]
        print(f"🆕 Nuevos a insertar: {len(df_aux)}")

        if not df_aux.empty:
            with engine.begin() as conn:
                df_aux.to_sql(tabla_sql, conn, if_exists='append', index=False)
            print(f"✅ Insertados en '{tabla_sql}': {len(df_aux)}")
        else:
            print(f"⚠️ Todos los registros ya existen en '{tabla_sql}'.")
    except SQLAlchemyError as e:
        print(f"❌ Error SQL al insertar en '{tabla_sql}': {e}")
    except Exception as e:
        print(f"❌ Error al insertar en '{tabla_sql}': {e}")

# ==============================
# 6️⃣ Insertar tablas auxiliares
# ==============================
insertar_auxiliar(df, 'tipo_identificacion', 'tipo_identificacion', 'nombre_tipo')
insertar_auxiliar(df, 'provincia', 'provincia', 'nombre_provincia')
insertar_auxiliar(df, 'institucion_financiera', 'institucion_financiera', 'nombre_institucion')
insertar_auxiliar(df, 'desc_forma_pago', 'forma_pago', 'desc_forma_pago')
insertar_auxiliar(df, 'id_subproducto', 'subproducto', 'id_subproducto')
insertar_auxiliar(df, 'id_ciclo', 'ciclo', 'id_ciclo')

# ==============================
# 7️⃣ Insertar ciudad
# ==============================
try:
    print("\n📋 Tabla: ciudad")
    prov_map = pd.read_sql(text("SELECT id_provincia, nombre_provincia FROM provincia"), engine)
    prov_map['nombre_provincia'] = prov_map['nombre_provincia'].astype(str).str.strip().str.upper()

    df_ciudad = df[['ciudad', 'provincia']].dropna().drop_duplicates()
    df_ciudad = df_ciudad.merge(prov_map, left_on='provincia', right_on='nombre_provincia')
    df_ciudad = df_ciudad.rename(columns={'ciudad': 'nombre_ciudad'})

    existentes = pd.read_sql(text("SELECT nombre_ciudad FROM ciudad"), engine)
    df_ciudad = df_ciudad[~df_ciudad['nombre_ciudad'].isin(existentes['nombre_ciudad'])]
    print(f"🆕 Nuevos a insertar: {len(df_ciudad)}")

    if not df_ciudad.empty:
        with engine.begin() as conn:
            df_ciudad[['nombre_ciudad', 'id_provincia']].to_sql('ciudad', conn, if_exists='append', index=False)
        print(f"✅ Insertados en 'ciudad': {len(df_ciudad)}")
    else:
        print(f"⚠️ Todos los registros ya existen en 'ciudad'.")
except Exception as e:
    print(f"❌ Error insertando ciudades: {e}")

# ==============================
# 8️⃣ Insertar planes
# ==============================
try:
    df_plan = df[['id_plan', 'descripcion_plan']].dropna().drop_duplicates()
    df_plan['id_plan'] = df_plan['id_plan'].astype(str).str.strip().str.upper()
    df_plan['descripcion_plan'] = df_plan['descripcion_plan'].astype(str).str.strip()

    existentes = pd.read_sql(text("SELECT id_plan FROM plan"), engine)
    existentes['id_plan'] = existentes['id_plan'].astype(str).str.strip().str.upper()

    df_plan_filtrado = df_plan[~df_plan['id_plan'].isin(existentes['id_plan'])]
    print(f"🆕 Nuevos a insertar: {len(df_plan_filtrado)}")

    if not df_plan_filtrado.empty:
        with engine.begin() as conn:
            df_plan_filtrado.to_sql('plan', conn, if_exists='append', index=False)
        print(f"✅ Insertados en 'plan': {len(df_plan_filtrado)}")
    else:
        print("⚠️ Todos los registros ya existen en 'plan'.")
except Exception as e:
    print(f"❌ Error insertando planes: {e}")

# ==============================
# 9️⃣ Insertar clientes y cliente_plan_info
# ==============================
try:
    # Normalizar clientes
    df_cliente = df[['identificacion', 'nombre_completo', 'celular', 'fecha_alta']].copy()
    df_cliente['celular'] = df_cliente['celular'].astype(str).str.strip().apply(lambda x: x if x.startswith('0') else '0'+x)
    df_cliente['fecha_alta'] = pd.to_datetime(df_cliente['fecha_alta'], errors='coerce', dayfirst=True)
    df_cliente['id_tipo_ident'] = df['id_tipo_ident']
    df_cliente['id_provincia'] = df['id_provincia']
    df_cliente['id_ciudad'] = df['id_ciudad']

    print(f"🔢 Registros a insertar en cliente: {len(df_cliente)}")
    with engine.begin() as conn:
        df_cliente.to_sql('cliente', conn, if_exists='append', index=False)
    print(f"✅ Insertados en 'cliente': {len(df_cliente)}")

    # Cargar auxiliares para ids
    def cargar_tabla_auxiliar(query, columna_clave):
        tabla = pd.read_sql(text(query), engine)
        tabla[columna_clave] = tabla[columna_clave].astype(str).str.strip().str.upper()
        return tabla.drop_duplicates(subset=[columna_clave])

    tipo_map = cargar_tabla_auxiliar('SELECT id_tipo_ident, nombre_tipo FROM tipo_identificacion', 'nombre_tipo')
    prov_map = cargar_tabla_auxiliar('SELECT id_provincia, nombre_provincia FROM provincia', 'nombre_provincia')
    ciudad_map = cargar_tabla_auxiliar('SELECT id_ciudad, nombre_ciudad FROM ciudad', 'nombre_ciudad')
    inst_map = cargar_tabla_auxiliar('SELECT id_institucion, nombre_institucion FROM institucion_financiera', 'nombre_institucion')
    pago_map = cargar_tabla_auxiliar('SELECT id_forma_pago, desc_forma_pago FROM forma_pago', 'desc_forma_pago')

    # Merge auxiliares
    df = df.merge(tipo_map, left_on='tipo_identificacion', right_on='nombre_tipo', how='left')
    df = df.merge(prov_map, left_on='provincia', right_on='nombre_provincia', how='left')
    df = df.merge(ciudad_map, left_on='ciudad', right_on='nombre_ciudad', how='left')
    df = df.merge(inst_map, left_on='institucion_financiera', right_on='nombre_institucion', how='left')
    df = df.merge(pago_map, left_on='desc_forma_pago', right_on='desc_forma_pago', how='left')

    # Normalizar celular
    def normalizar_celular(c):
        c = str(c).strip()
        if c.startswith('0'):
            return c
        elif len(c) == 9:
            return '0'+c
        return c

    cliente_map = pd.read_sql(text('SELECT id_cliente, identificacion, celular FROM cliente'), engine)
    cliente_map['identificacion'] = cliente_map['identificacion'].astype(str).str.strip().str.upper()
    cliente_map['celular'] = cliente_map['celular'].apply(normalizar_celular)
    cliente_map = cliente_map.drop_duplicates(subset=['identificacion', 'celular'], keep='last')

    df['identificacion'] = df['identificacion'].astype(str).str.strip().str.upper()
    df['celular'] = df['celular'].apply(normalizar_celular)
    df = df.merge(cliente_map, on=['identificacion','celular'], how='left')

    # Insertar subproductos faltantes
    subproductos_existentes = pd.read_sql(text('SELECT id_subproducto FROM subproducto'), engine)
    subproductos_existentes['id_subproducto'] = subproductos_existentes['id_subproducto'].astype(str).str.strip().str.upper()
    df_subproducto = pd.DataFrame({'id_subproducto': df['id_subproducto'].dropna().unique()})
    df_subproducto['id_subproducto'] = df_subproducto['id_subproducto'].astype(str).str.strip().str.upper()
    df_subproducto_nuevos = df_subproducto[~df_subproducto['id_subproducto'].isin(subproductos_existentes['id_subproducto'])]
    if not df_subproducto_nuevos.empty:
        with engine.begin() as conn:
            df_subproducto_nuevos.to_sql('subproducto', conn, if_exists='append', index=False)
        print(f"✅ Insertados en 'subproducto': {len(df_subproducto_nuevos)}")
    else:
        print("⚠️ Todos los subproductos ya existen.")

    # Insertar cliente_plan_info
    df_plan_info = df[['id_cliente','id_plan','id_subproducto','id_ciclo','id_forma_pago',
                       'id_institucion','tb','categoria1','id_periodo']].copy()
    df_plan_info = df_plan_info.dropna(subset=['id_cliente','id_plan','id_subproducto','id_ciclo',
                                               'id_forma_pago','id_institucion','id_periodo'])
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

    print(f"🔢 Registros válidos a insertar en cliente_plan_info: {len(df_plan_info)}")
    if not df_plan_info.empty:
        with engine.begin() as conn:
            df_plan_info.to_sql('cliente_plan_info', conn, if_exists='append', index=False)
        print(f"✅ Insertados en 'cliente_plan_info': {len(df_plan_info)}")
    else:
        print("⚠️ No hay registros válidos para insertar en 'cliente_plan_info'.")
except Exception as e:
    print(f"❌ Error insertando cliente_plan_info: {e}")
