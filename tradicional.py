import pandas as pd
import glob
import os
import sys
from sqlalchemy import create_engine

# ==============================
# 1️⃣ Conexión a PostgreSQL
# ==============================
usuario = 'postgres'
contraseña = 'pasante'
host = 'localhost'
puerto = '5432'
base_datos = 'tradicional'

connection_string = f'postgresql+psycopg2://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}'
engine = create_engine(connection_string)

# ==============================
# 2️⃣ Leer todos los Excel de la carpeta
# ==============================
carpeta_principal = r'C:\Users\pasante.ti2\Documents\MOVISTAR\TRADICIONAL'
rutas_excel = glob.glob(os.path.join(carpeta_principal, '**', '*.xlsx'), recursive=True)

if not rutas_excel:
    sys.exit("❌ No se encontraron archivos Excel en la carpeta indicada.")

df_list = []

# ==============================
# 3️⃣ Leer hojas y normalizar columnas
# ==============================
for ruta_excel in rutas_excel:
    nombre_archivo = os.path.basename(ruta_excel)
    if nombre_archivo.startswith('~$'):
        continue

    try:
        hojas = pd.read_excel(ruta_excel, sheet_name=None, dtype=str)

        for nombre_hoja, df_hoja in hojas.items():
            # Normalizar nombres de columnas
            df_hoja.columns = [str(col).lower().split('_m')[0].strip() for col in df_hoja.columns]

            # Columnas requeridas
            columnas_requeridas = [
                'año','mes','texto_extraido','nombre_completo','identificacion',
                'celular','fecha_baja','dpa_provincia','operadora_destino','deuda_movistar'
            ]
            for c in columnas_requeridas:
                if c not in df_hoja.columns:
                    df_hoja[c] = None

            # Limpieza de celular
            df_hoja['celular'] = df_hoja['celular'].astype(str).str.replace(r'\.0$', '', regex=True)
            df_hoja['celular'] = df_hoja['celular'].apply(
                lambda x: x if x.startswith('0') else '0'+x if x and x.isdigit() else x
            )

            # Limpieza de texto
            for c in ['nombre_completo','identificacion','dpa_provincia','operadora_destino']:
                df_hoja[c] = df_hoja[c].apply(lambda x: x.strip() if pd.notna(x) else '')

            # Normalizar mes y texto_extraido
            df_hoja['mes'] = df_hoja['mes'].astype(str).str.strip().str.upper()
            df_hoja['texto_extraido'] = df_hoja['texto_extraido'].fillna('').astype(str).str.strip()

            df_list.append(df_hoja)

    except Exception as e:
        print(f"⚠️ Error leyendo {nombre_archivo}: {e}")

if not df_list:
    sys.exit("❌ No se pudo leer ningún Excel correctamente.")

# ==============================
# 4️⃣ Combinar DataFrames
# ==============================
df = pd.concat(df_list, ignore_index=True)
print(f"📊 Total registros combinados: {len(df)}")

# ==============================
# 5️⃣ Normalizar año
# ==============================
df['año'] = df['año'].fillna('2025').astype(str).str.strip()

# ==============================
# 6️⃣ Obtener IDs de año y mes
# ==============================
anio_db = pd.read_sql('SELECT id_anio, valor FROM anio', engine)
anio_db['valor'] = anio_db['valor'].astype(str).str.strip()

mes_db = pd.read_sql('SELECT id_mes, nombre_mes FROM mes', engine)
mes_db['nombre_mes'] = mes_db['nombre_mes'].astype(str).str.strip().str.upper()

df = df.merge(anio_db, left_on='año', right_on='valor', how='left')
df = df.merge(mes_db, left_on='mes', right_on='nombre_mes', how='left')

# ==============================
# 7️⃣ Insertar periodos únicos
# ==============================
df['texto_extraido'] = df['texto_extraido'].replace('', None)
df_periodos = df[['id_anio','id_mes','texto_extraido']].dropna(subset=['texto_extraido']).drop_duplicates()
df_periodos.to_sql('periodo_carga', engine, if_exists='append', index=False, method='multi')
print(f"✅ Periodos únicos insertados: {len(df_periodos)}")

# ==============================
# 8️⃣ Asignar id_periodo
# ==============================
df_periodos_db = pd.read_sql('SELECT * FROM periodo_carga', engine)
periodo_map = df_periodos_db.set_index(['id_anio','id_mes','texto_extraido'])['id_periodo'].to_dict()
df['id_periodo'] = df.apply(lambda r: periodo_map.get((r['id_anio'], r['id_mes'], r['texto_extraido'])), axis=1)

# ==============================
# 9️⃣ Provincias
# ==============================
prov_db = pd.read_sql('SELECT id_provincia, nombre_provincia FROM provincia', engine)
prov_excel = df['dpa_provincia'].str.strip().str.upper().dropna().unique()
prov_nuevas = [p for p in prov_excel if p and p not in prov_db['nombre_provincia'].str.upper().values]

if prov_nuevas:
    df_prov_nuevas = pd.DataFrame({'nombre_provincia': prov_nuevas})
    df_prov_nuevas.to_sql('provincia', engine, if_exists='append', index=False, method='multi')

prov_db = pd.read_sql('SELECT id_provincia, nombre_provincia FROM provincia', engine)
prov_map = dict(zip(prov_db['nombre_provincia'].str.upper(), prov_db['id_provincia']))
df['id_provincia'] = df['dpa_provincia'].str.strip().str.upper().map(prov_map)

# ==============================
# 🔟 Insertar clientes (sin duplicados)
# ==============================
df['fecha_baja'] = pd.to_datetime(df['fecha_baja'], errors='coerce').dt.date
df_cliente = df[['identificacion','nombre_completo','celular','id_provincia',
                 'operadora_destino','deuda_movistar','fecha_baja']].drop_duplicates(subset=['identificacion'])

df_clientes_db = pd.read_sql('SELECT id_cliente, identificacion FROM cliente', engine)
clientes_nuevos = df_cliente[~df_cliente['identificacion'].isin(df_clientes_db['identificacion'])]

if not clientes_nuevos.empty:
    clientes_nuevos.to_sql('cliente', engine, if_exists='append', index=False, method='multi')
    print(f"✅ Clientes nuevos insertados: {len(clientes_nuevos)}")
else:
    print("✅ No hay clientes nuevos para insertar")

# ==============================
# 1️⃣1️⃣ Insertar cliente_plan_info
# ==============================
df_clientes_db = pd.read_sql('SELECT id_cliente, identificacion FROM cliente', engine)
df['id_cliente'] = df['identificacion'].map(dict(zip(df_clientes_db['identificacion'], df_clientes_db['id_cliente'])))

df_plan_info = df[['id_cliente','id_periodo']].dropna().drop_duplicates()
if not df_plan_info.empty:
    df_plan_info.to_sql('cliente_plan_info', engine, if_exists='append', index=False, method='multi')
    print(f"✅ Cliente_plan_info insertados: {len(df_plan_info)}")
else:
    print("⚠️ No hay registros válidos para insertar en cliente_plan_info")

# ==============================
# 1️⃣2️⃣ Resumen final
# ==============================
resumen_mes = df.groupby('mes').size().reset_index(name='registros')
print("\n📊 Registros por mes:")
print(resumen_mes)

print("\n🎉 ¡Carga completa en PostgreSQL con todas las relaciones correctas!")
