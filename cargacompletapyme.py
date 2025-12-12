import pandas as pd # Requiere: pip install pandas openpyxl sqlalchemy psycopg2-binary
import logging # Requiere: pip install pandas openpyxl sqlalchemy psycopg2-binary
import os # Requiere: pip install pandas openpyxl sqlalchemy psycopg2-binary


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# ==========================================

def cargar_datos(engine, ruta_excel):
    """
     
    Función principal para cargar datos desde Excel a PostgreSQL
    usando la conexión engine pasada desde pyme.py 
    """
    # ----------------------------- 
    # 1️⃣ Leer Excel automáticamente 
    # -----------------------------
    # Leer la primera hoja del archivo Excel
    try:
        excel = pd.ExcelFile(ruta_excel)
        nombre_hoja = excel.sheet_names[0]
        df = pd.read_excel(excel, sheet_name=nombre_hoja)
        df.columns = [col.lower() for col in df.columns]
        logging.info(f"📥 Hoja leída correctamente: {nombre_hoja}")
    except Exception as e:
        logging.exception("❌ Error leyendo el Excel.")
        raise
 
    # ----------------------------- 
    # 2️⃣ Normalizar columnas de periodo
    # -----------------------------
    try:
        df['año'] = df['año'].astype(str).str.strip()
        df['mes'] = df['mes'].astype(str).str.strip().str.upper()
        df['texto_extraido'] = df['texto_extraido'].astype(str).str.strip()
    except Exception as e:
        logging.exception("❌ Error normalizando columnas de periodo.")
        raise

    # -----------------------------
    # 3️⃣ Obtener id_anio
    # -----------------------------
    try:
        año = str(int(float(df['año'].dropna().unique()[0]))).strip()
        anio_result = pd.read_sql(f"SELECT id_anio FROM anio WHERE valor = '{año}'", engine)
        if anio_result.empty:
            raise ValueError(f"Año '{año}' no encontrado en tabla 'anio'.")
        id_anio = anio_result.iloc[0]['id_anio']
    except Exception as e:
        logging.exception("❌ Error obteniendo id_anio.")
        raise

    # -----------------------------
    # 4️⃣ Crear períodos y obtener id_periodo
    # -----------------------------
    periodos = []
    meses_unicos = df['mes'].dropna().unique()
    texto_extraido = df['texto_extraido'].dropna().unique()[0] if len(df['texto_extraido'].dropna()) > 0 else ''

    for mes in meses_unicos:
        try:
            mes_result = pd.read_sql(f"SELECT id_mes FROM mes WHERE nombre_mes = '{mes}'", engine)
            if mes_result.empty:
                raise ValueError(f"Mes '{mes}' no encontrado en tabla 'mes'.")
            id_mes = mes_result.iloc[0]['id_mes']

            # Verificar existencia del período

            query = f"""
                SELECT id_periodo 
                FROM periodo_carga 
                WHERE id_anio = {id_anio} 
                AND id_mes = {id_mes} 
                AND texto_extraido = '{texto_extraido}'
                AND nombre_base = '{os.path.basename(ruta_excel).replace(".xlsx", "")}'
            """
            existente = pd.read_sql(query, engine)
            if not existente.empty:
                id_periodo = existente.iloc[0]['id_periodo']
                logging.info(f"ℹ️ Período ya existente: {mes} {año} → id_periodo = {id_periodo}")
            else:
                df_periodo = pd.DataFrame([{
                'id_anio': id_anio,
                'id_mes': id_mes,
                'texto_extraido': texto_extraido,
                'nombre_base': os.path.basename(ruta_excel).replace(".xlsx", "")
            }])

                with engine.begin() as conn:
                    df_periodo.to_sql('periodo_carga', conn, if_exists='append', index=False)
                    id_periodo = pd.read_sql('SELECT MAX(id_periodo) AS id FROM periodo_carga', conn).iloc[0]['id']
                    logging.info(f"🆕 Nuevo período insertado: {mes} {año} → id_periodo = {id_periodo}")
        except Exception as e:
            logging.exception(f"❌ Error manejando período para {mes}.")
            raise

        periodos.append({'mes': mes, 'id_periodo': id_periodo})

    # Asignar id_periodo a cada fila
    periodo_map = pd.DataFrame(periodos)
    df = df.merge(periodo_map, on='mes', how='left')

    # -----------------------------
    # 5️⃣ Normalizar columnas clave
    # -----------------------------
    cols_clave = ['identificacion', 'tipo_identificacion', 'provincia', 'ciudad',
                  'institucion_financiera', 'desc_forma_pago', 'id_plan', 'id_ciclo', 'id_subproducto']
    for col in cols_clave:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    # -----------------------------
    # 6️⃣ Función auxiliar para tablas auxiliares
    # -----------------------------
    def insertar_auxiliar(df_origen, columna, tabla_sql, columna_sql, conn):
        logging.info(f"\n📋 Tabla: {tabla_sql}")
        df_aux = pd.DataFrame({columna_sql: df_origen[columna].dropna().drop_duplicates()})
        df_aux[columna_sql] = df_aux[columna_sql].astype(str).str.strip().str.upper()
        existentes = pd.read_sql(f'SELECT {columna_sql} FROM {tabla_sql}', conn)
        existentes[columna_sql] = existentes[columna_sql].astype(str).str.strip().str.upper()
        df_aux = df_aux[~df_aux[columna_sql].isin(existentes[columna_sql])]
        logging.info(f"🆕 Nuevos a insertar: {len(df_aux)}")
        if not df_aux.empty:
            df_aux.to_sql(tabla_sql, conn, if_exists='append', index=False)
            logging.info(f"✅ Insertados en '{tabla_sql}': {len(df_aux)}")
        else:
            logging.info(f"⚠️ Todos los registros ya existen en '{tabla_sql}'.")

    # -----------------------------
    # 7️⃣ Insertar tablas auxiliares y principales
    # -----------------------------
    try:
        with engine.begin() as conn:
            insertar_auxiliar(df, 'tipo_identificacion', 'tipo_identificacion', 'nombre_tipo', conn)
            insertar_auxiliar(df, 'provincia', 'provincia', 'nombre_provincia', conn)
            insertar_auxiliar(df, 'institucion_financiera', 'institucion_financiera', 'nombre_institucion', conn)
            insertar_auxiliar(df, 'desc_forma_pago', 'forma_pago', 'desc_forma_pago', conn)
            insertar_auxiliar(df, 'id_subproducto', 'subproducto', 'id_subproducto', conn)
            insertar_auxiliar(df, 'id_ciclo', 'ciclo', 'id_ciclo', conn)

            # Insertar ciudad
            prov_map = pd.read_sql('SELECT id_provincia, nombre_provincia FROM provincia', conn)
            prov_map['nombre_provincia'] = prov_map['nombre_provincia'].astype(str).str.strip().str.upper()
            df_ciudad = df[['ciudad', 'provincia']].dropna().drop_duplicates()
            df_ciudad = df_ciudad.merge(prov_map, left_on='provincia', right_on='nombre_provincia')
            df_ciudad = df_ciudad.rename(columns={'ciudad': 'nombre_ciudad'})
            existentes = pd.read_sql('SELECT nombre_ciudad FROM ciudad', conn)
            df_ciudad = df_ciudad[~df_ciudad['nombre_ciudad'].isin(existentes['nombre_ciudad'])]
            if not df_ciudad.empty:
                df_ciudad[['nombre_ciudad', 'id_provincia']].to_sql('ciudad', conn, if_exists='append', index=False)

            # Insertar planes
            df_plan = df[['id_plan', 'descripcion_plan']].dropna().drop_duplicates()
            df_plan['id_plan'] = df_plan['id_plan'].astype(str).str.strip().str.upper()
            df_plan['descripcion_plan'] = df_plan['descripcion_plan'].astype(str).str.strip()
            existentes = pd.read_sql('SELECT id_plan FROM plan', conn)
            existentes['id_plan'] = existentes['id_plan'].astype(str).str.strip().str.upper()
            df_plan_filtrado = df_plan[~df_plan['id_plan'].isin(existentes['id_plan'])]
            if not df_plan_filtrado.empty:
                df_plan_filtrado.to_sql('plan', conn, if_exists='append', index=False)

            # Mapear auxiliares para cliente y cliente_plan_info
            def cargar_tabla_auxiliar(query, columna_clave, conn):
                tabla = pd.read_sql(query, conn)
                tabla[columna_clave] = tabla[columna_clave].astype(str).str.strip().str.upper()
                return tabla.drop_duplicates(subset=[columna_clave])

            tipo_map = cargar_tabla_auxiliar('SELECT id_tipo_ident, nombre_tipo FROM tipo_identificacion', 'nombre_tipo', conn)
            prov_map = cargar_tabla_auxiliar('SELECT id_provincia, nombre_provincia FROM provincia', 'nombre_provincia', conn)
            ciudad_map = cargar_tabla_auxiliar('SELECT id_ciudad, nombre_ciudad FROM ciudad', 'nombre_ciudad', conn)
            inst_map = cargar_tabla_auxiliar('SELECT id_institucion, nombre_institucion FROM institucion_financiera', 'nombre_institucion', conn)
            pago_map = cargar_tabla_auxiliar('SELECT id_forma_pago, desc_forma_pago FROM forma_pago', 'desc_forma_pago', conn)

            def merge_con_log(df_local, tabla_aux, columna_df, columna_aux, nombre_tabla):
                antes = len(df_local)
                df_local = df_local.merge(tabla_aux, left_on=columna_df, right_on=columna_aux, how='left')
                logging.info(f"🔄 Merge con {nombre_tabla}: antes={antes}, después={len(df_local)}")
                return df_local

            df = merge_con_log(df, tipo_map, 'tipo_identificacion', 'nombre_tipo', 'tipo_identificacion')
            df = merge_con_log(df, prov_map, 'provincia', 'nombre_provincia', 'provincia')
            df = merge_con_log(df, ciudad_map, 'ciudad', 'nombre_ciudad', 'ciudad')
            df = merge_con_log(df, inst_map, 'institucion_financiera', 'nombre_institucion', 'institucion_financiera')
            df = merge_con_log(df, pago_map, 'desc_forma_pago', 'desc_forma_pago', 'forma_pago')

            # Insertar clientes
            df_cliente = df[['identificacion', 'nombre_completo', 'celular', 'fecha_alta']].copy()
            df_cliente['celular'] = df_cliente['celular'].astype(str).str.strip().apply(lambda x: x if x.startswith('0') else '0'+x)
            df_cliente['fecha_alta'] = pd.to_datetime(df_cliente['fecha_alta'], errors='coerce', dayfirst=True)
            df_cliente['id_tipo_ident'] = df['id_tipo_ident']
            df_cliente['id_provincia'] = df['id_provincia']
            df_cliente['id_ciudad'] = df['id_ciudad']
            if not df_cliente.empty:
                df_cliente.to_sql('cliente', conn, if_exists='append', index=False)

            # Mapear id_cliente
            def normalizar_celular_local(c):
                c = str(c).strip()
                if c.startswith('0'):
                    return c
                elif len(c) == 9:
                    return '0'+c
                return c

            cliente_map = pd.read_sql('SELECT id_cliente, identificacion, celular FROM cliente', conn)
            cliente_map['identificacion'] = cliente_map['identificacion'].astype(str).str.strip().str.upper()
            cliente_map['celular'] = cliente_map['celular'].apply(normalizar_celular_local)
            cliente_map = cliente_map.drop_duplicates(subset=['identificacion', 'celular'], keep='last')
            df['identificacion'] = df['identificacion'].astype(str).str.strip().str.upper()
            df['celular'] = df['celular'].apply(normalizar_celular_local)
            df = df.merge(cliente_map, on=['identificacion', 'celular'], how='left')

            # Insertar cliente_plan_info
            df_plan_info = df[['id_cliente', 'id_plan', 'id_subproducto', 'id_ciclo', 'id_forma_pago',
                               'id_institucion', 'tb', 'categoria1', 'id_periodo']].copy()
            df_plan_info = df_plan_info.dropna(subset=['id_cliente', 'id_plan', 'id_subproducto', 'id_ciclo', 'id_forma_pago', 'id_institucion', 'id_periodo'])
            if not df_plan_info.empty:
                df_plan_info.to_sql('cliente_plan_info', conn, if_exists='append', index=False)

            logging.info("🎯 Proceso terminado correctamente. Todas las inserciones confirmadas (commit).")

    except Exception as e:
        logging.exception("❌ Error inesperado durante la carga. Se aplicó ROLLBACK automático si correspondía.")
        raise
 