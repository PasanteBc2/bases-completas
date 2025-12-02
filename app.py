from flask import Flask, render_template, request
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import logging
import io
from flask import send_file
import traceback 
import re

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==============================
# Configuración de conexión
# ==============================
usuario = "analista"
contraseña = "2025Anal1st@"
host = "192.168.10.116"
puerto = 5432

map_nombres_bases = {
    'base_pyme': 'BcorpPymePrueba',
    'pospago':   'BcorpPostPrueba',
    'prepago':   'BcorpPrePrueba'
}

map_base_to_id = {
    'base_pyme': 3,
    'pospago': 1,
    'prepago': 2
}

def create_engine_for(db_name):
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=usuario,
        password=contraseña,
        host=host,
        port=puerto,
        database=db_name
    )
    eng = create_engine(url, pool_pre_ping=True)
    try:
        with eng.connect() as conn:
            logging.info(f"✅ Conexión OK → DB={db_name}")
        return eng
    except Exception as e:
        logging.error(f"❌ Error conexión → DB={db_name}: {e}")
        raise

# Crear engines para todas las bases
engines = {name: create_engine_for(db) for name, db in map_nombres_bases.items()}

# ==============================
# Función unificada de búsqueda
# ==============================
def buscar_en_bases_por_identificacion(valores, tipo):
    resultados = pd.DataFrame()

    # Convertir a tupla si viene como string
    if isinstance(valores, str):
        valores = tuple([v.strip() for v in valores.split(',')])
    elif isinstance(valores, list):
        valores = tuple(valores)

    try:
        for base, engine in engines.items():
            if tipo == '1':  # ORIGEN
                filtro_mes_pyme = "AND m.id_mes >= 10" if base == "base_pyme" else ""
                
                query = f"""
                    SELECT
                        c.identificacion,
                        c.celular,
                        c.nombre_completo,
                        a.valor AS año,
                        m.nombre_mes AS mes,
                        '{base}' AS origen
                    FROM cliente_plan_info cp
                    JOIN cliente c ON c.id_cliente = cp.id_cliente
                    JOIN periodo_carga p ON p.id_periodo = cp.id_periodo
                    JOIN anio a ON a.id_anio = p.id_anio
                    JOIN mes m ON m.id_mes = p.id_mes
                    WHERE (c.identificacion IN :valores OR c.celular IN :valores)
                    {filtro_mes_pyme}
                    ORDER BY c.identificacion, c.celular, a.valor, m.id_mes
                """
                df = pd.read_sql(text(query), engine, params={"valores": valores})

            elif tipo == '2':  # TITULARIDAD
                query = """
                    SELECT
                        c.identificacion,
                        c.celular,
                        c.nombre_completo,
                        p.nombre_base
                    FROM cliente_plan_info cp
                    JOIN cliente c ON c.id_cliente = cp.id_cliente
                    JOIN periodo_carga p ON cp.id_periodo = p.id_periodo
                    WHERE c.identificacion IN :valores OR c.celular IN :valores
                    ORDER BY c.identificacion, c.celular
                """
                df = pd.read_sql(text(query), engine, params={"valores": valores})

            if not df.empty:
                resultados = pd.concat([resultados, df], ignore_index=True)

    except Exception as e:
        logging.error(f"❌ Error consultando bases: {e}")

    return resultados.to_dict(orient='records') if not resultados.empty else []

# ==============================
# Rutas Flask
# ==============================
@app.route('/')
def home():
    return render_template('index.html')


# --------------------------------------------------------
# Ruta de búsqueda
# --------------------------------------------------------
@app.route('/buscar', methods=['GET', 'POST'])
def buscar():
    tipo = request.args.get('tipo', '1')  # 1=Origen, 2=Titularidad
    resultados = []
    mensaje = ""
 

    valores_input = request.form.get('valores', '').strip()
    valores = [v for v in re.split(r'[\s,]+', valores_input) if v]


    resultados = buscar_en_bases_por_identificacion(valores, tipo)

    if not resultados:
        mensaje = "❌ No se encontraron resultados."



    return render_template('buscar.html', tipo=tipo, resultados=resultados, mensaje=mensaje, valores_input=valores_input)


@app.route('/buscar_ciclo', methods=['GET', 'POST'])
def buscar_ciclo():
    engine_pospago = engines['pospago']

    # Obtener listas para los select
    try:
        with engine_pospago.connect() as conn:
            años = pd.read_sql("SELECT DISTINCT valor FROM anio ORDER BY valor DESC", conn)
            ciclos = pd.read_sql("SELECT DISTINCT id_ciclo FROM cliente_plan_info ORDER BY id_ciclo", conn)
    except Exception as e:
        logging.error(f"❌ Error cargando años/ciclos: {e}")
        años = ciclos = pd.DataFrame()

    año_sel = []
    ciclo_sel = None
    resultados = []
    mensaje = ""

    if request.method == 'POST':
        año_sel = request.form.getlist('anio')  # lista de años
        ciclo_sel = request.form.get('ciclo')

        if not año_sel or not ciclo_sel:
            mensaje = "⚠️ Seleccione al menos un año y un ciclo."
        else:
            resultados_df = pd.DataFrame()
            try:
                with engine_pospago.connect() as conn:
                    for anio in año_sel:
                        df = pd.read_sql(
                            text("""
                                SELECT 
                                    c.identificacion,
                                    c.celular,
                                    c.nombre_completo,
                                    cp.id_ciclo AS ciclo,
                                    a.valor AS anio
                                FROM cliente c
                                JOIN cliente_plan_info cp ON c.id_cliente = cp.id_cliente
                                JOIN periodo_carga p ON cp.id_periodo = p.id_periodo
                                JOIN anio a ON p.id_anio = a.id_anio
                                WHERE a.valor::text = :anio
                                  AND cp.id_ciclo = :ciclo
                            """),
                            conn,
                            params={"anio": str(anio), "ciclo": int(ciclo_sel)}
                        )
                        if not df.empty:
                            resultados_df = pd.concat([resultados_df, df], ignore_index=True)

                if resultados_df.empty:
                    mensaje = "❌ No se encontraron registros."
                else:
                    # Limitar la vista previa a 200 registros
                    resultados = resultados_df.head(200).to_dict(orient='records')
                    mensaje = f"⚡ Mostrando {len(resultados)} de {len(resultados_df)} registros. Para ver todos, descargue el Excel."

            except Exception as e:
                logging.error(f"❌ Error consulta ciclo-pospago: {e}")
                mensaje = "❌ Error consultando la base pospago."

    return render_template(
        'buscar_ciclo.html',
        años=años['valor'].tolist() if not años.empty else [],
        ciclos=ciclos['id_ciclo'].tolist() if not ciclos.empty else [],
        año_sel=año_sel,
        ciclo_sel=ciclo_sel,
        resultados=resultados,
        mensaje=mensaje
    )

@app.route('/descargar_excel')
def descargar_excel():
    años = request.args.getlist("anio")  # múltiples años
    ciclo = request.args.get("ciclo")

    try:
        engine = engines['pospago']

        resultados_df = pd.DataFrame()
        with engine.connect() as conn:
            for anio in años:
                df = pd.read_sql(
                    text("""
                        SELECT 
                            c.identificacion,
                            c.celular,
                            c.nombre_completo,
                            cp.id_ciclo AS ciclo,
                            a.valor AS anio
                        FROM cliente c
                        JOIN cliente_plan_info cp ON c.id_cliente = cp.id_cliente
                        JOIN periodo_carga p ON cp.id_periodo = p.id_periodo
                        JOIN anio a ON p.id_anio = a.id_anio
                        WHERE a.valor::text = :anio
                          AND cp.id_ciclo = :ciclo
                    """),
                    conn,
                    params={"anio": str(anio), "ciclo": int(ciclo)}
                )
                if not df.empty:
                    resultados_df = pd.concat([resultados_df, df], ignore_index=True)

        if resultados_df.empty:
            return "❌ No se encontraron registros para descargar.", 404

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            resultados_df.to_excel(writer, index=False, sheet_name="Datos")
        output.seek(0)

        return send_file(
            output,
            as_attachment=True,
            download_name=f"base_anios_{'_'.join(años)}_ciclo_{ciclo}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        logging.error(traceback.format_exc())
        return f"Error exportando: {e}", 500


# --------------------------------------------------------
# Descarga Excel origen
# --------------------------------------------------------
@app.route('/descargar_excel_origen')
def descargar_excel_origen():
    try:
        resultados_df = pd.read_pickle("/tmp/resultados_buscar_origen.pkl")
        if resultados_df.empty:
            return "❌ No hay registros para descargar.", 404

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            resultados_df.to_excel(writer, index=False, sheet_name="Datos")
        output.seek(0)

        return send_file(
            output,
            as_attachment=True,
            download_name="consulta_origen_completa.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        logging.error(traceback.format_exc())
        return f"Error exportando: {e}", 500
    

# titularidad

@app.route('/descargar_excel_titularidad')
def descargar_excel_titularidad():
    try:
        # Recuperar los resultados de titularidad guardados temporalmente
        resultados_df = pd.read_pickle("/tmp/resultados_buscar_titularidad.pkl")
        if resultados_df.empty:
            return "❌ No hay registros para descargar.", 404

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            resultados_df.to_excel(writer, index=False, sheet_name="Datos")
        output.seek(0)

        return send_file(
            output,
            as_attachment=True,
            download_name="consulta_titularidad_completa.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        logging.error(traceback.format_exc())
        return f"Error exportando: {e}", 500


# ==============================
# Ejecutar app
# ==============================
if __name__ == '__main__':
    app.run(debug=True, port=5000)