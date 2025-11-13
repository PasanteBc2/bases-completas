from flask import Flask, render_template, request
from sqlalchemy import create_engine, text
import pandas as pd
from urllib.parse import quote_plus
import logging

# ==============================
# Configuración general
# ==============================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = Flask(__name__, template_folder='vistas_html')

# ==============================
# Conexión a la base de datos BAS
# ==============================
usuario = 'postgres'
contraseña = quote_plus('pasante')
host = 'localhost'
puerto = '5432'
base_datos_bas = 'BAS'

try:
    engine_bas = create_engine(
        f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_datos_bas}',
        connect_args={"options": "-c client_encoding=UTF8"}
    )
    with engine_bas.connect() as conn:
        conn.execute(text("SELECT 1"))
    logging.info("✅ Conexión exitosa a la base de datos 'BAS'.")
except Exception as e:
    logging.error(f"❌ Error al conectar BAS: {e}")

# ==============================
# Conexión a la base de datos POSPAGO
# ==============================
base_datos_pospago = 'pospago'

try:
    engine_pospago = create_engine(
        f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_datos_pospago}',
        connect_args={"options": "-c client_encoding=UTF8"}
    )
    with engine_pospago.connect() as conn:
        conn.execute(text("SELECT 1"))
    logging.info("✅ Conexión exitosa a la base de datos 'POSPAGO'.")
except Exception as e:
    logging.error(f"❌ Error al conectar POSPAGO: {e}")
    engine_pospago = None  # Para evitar que rompa la app si no se conecta

# ==============================
# Página de inicio
# ==============================
@app.route('/')
def inicio():
    return render_template('inicio.html')

# ==============================
# Página de búsqueda por cédula/celular (BAS)
# ==============================
@app.route('/buscar', methods=['GET', 'POST'])
def buscar():
    tipo = request.args.get('tipo', '1')  # "1" = consulta completa, "2" = por origen

    if request.method == 'POST':
        valores_input = request.form.get('valores', '').strip()
        valores = [v.strip() for v in valores_input.replace('\n', ',').split(',') if v.strip()]

        if not valores:
            return render_template('index.html', tipo=tipo, mensaje="⚠️ Ingrese al menos un número o cédula.")
        if len(valores) > 5:
            return render_template('index.html', tipo=tipo, mensaje="⚠️ Solo se pueden consultar hasta 5 valores.")

        celulares = [v for v in valores if v.isdigit() and len(v) == 10 and v.startswith('09')]
        cedulas = [v for v in valores if v not in celulares]

        params, condiciones = {}, []

        if celulares:
            placeholders_cel = ', '.join([f":cel{i}" for i in range(len(celulares))])
            condiciones.append(f"c.celular IN ({placeholders_cel})")
            params.update({f"cel{i}": v for i, v in enumerate(celulares)})

        if cedulas:
            placeholders_ced = ', '.join([f":ced{i}" for i in range(len(cedulas))])
            condiciones.append(f"c.identificacion IN ({placeholders_ced})")
            params.update({f"ced{i}": v for i, v in enumerate(cedulas)})

        where_clause = " OR ".join(condiciones)

        # Consulta SQL según tipo
        if tipo == '1':
            query = text(f"""
                SELECT
                       c.identificacion,
                       c.celular,
                       c.nombre_completo,
                       c.texto_extraido,
                       a.valor AS año,
                       m.nombre_mes AS mes,
                       o.nombre_origen AS origen
                FROM cliente_consolidado c
                JOIN anio a ON a.id_anio = c.id_anio
                JOIN mes m ON m.id_mes = c.id_mes
                JOIN origen o ON o.id_origen = c.id_origen
                WHERE {where_clause}
                ORDER BY a.valor DESC, m.id_mes DESC;
            """)
        else:
            query = text(f"""
                SELECT
                       c.identificacion,
                       c.celular,
                       c.nombre_completo,
                       o.nombre_origen AS origen
                FROM cliente_consolidado c
                JOIN origen o ON o.id_origen = c.id_origen
                WHERE {where_clause}
                ORDER BY 
                    CASE o.nombre_origen 
                        WHEN 'PYME' THEN 1 
                        WHEN 'POSPAGO' THEN 2 
                        WHEN 'PREPAGO' THEN 3 
                        ELSE 99 
                    END,
                    c.id_anio DESC, 
                    c.id_mes DESC;
            """)

        try:
            df = pd.read_sql(query, engine_bas, params=params)
        except Exception as e:
            logging.error(f"❌ Error en la consulta SQL: {e}")
            return render_template('index.html', tipo=tipo, mensaje="❌ Error al consultar la base de datos.")

        if df.empty:
            return render_template('index.html', tipo=tipo, mensaje="❌ No se encontraron registros.", valores_input=valores_input)

        # Marcar duplicados y ordenar
        df['duplicado'] = df.duplicated(subset=['celular'], keep=False)
        orden_columnas = ['celular']
        if 'año' in df.columns and 'mes' in df.columns:
            orden_columnas += ['año', 'mes']
        df = df.sort_values(by=orden_columnas, ascending=[True] * len(orden_columnas))

        resultados = df.to_dict(orient='records')
        return render_template('index.html', tipo=tipo, resultados=resultados, valores_input=valores_input)

    return render_template('index.html', tipo=tipo)

# ==============================
# Búsqueda por Ciclo y Año (POSPAGO)
# ==============================
@app.route("/buscar_ciclo", methods=["GET", "POST"])
def buscar_ciclo():
    resultados = []
    mensaje = ""

    if engine_pospago is None:
        mensaje = "❌ No se pudo conectar a la base POSPAGO."
        return render_template("buscar_ciclo.html", anios=[], ciclos=[], resultados=[], mensaje=mensaje)

    # Traemos años y ciclos desde POSPAGO
    anios_df = pd.read_sql("SELECT * FROM anio ORDER BY valor", engine_pospago)
    ciclos_df = pd.read_sql("SELECT DISTINCT id_ciclo FROM cliente_plan_info ORDER BY id_ciclo", engine_pospago)

    if request.method == "POST":
        id_anio = request.form.get("anio")
        id_ciclo = request.form.get("ciclo")

        try:
            # Traemos todos los datos de POSPAGO
            query = """
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
                ORDER BY a.valor, cp.id_ciclo, c.nombre_completo;
            """
            df = pd.read_sql(query, engine_pospago)

            # Filtramos según selección del usuario
            if id_anio:
                df = df[df["anio"] == int(id_anio)]
            if id_ciclo:
                df = df[df["ciclo"] == int(id_ciclo)]

            resultados = df.to_dict(orient="records")
            if not resultados:
                mensaje = "❌ No se encontraron registros para este Año y Ciclo."

        except Exception as e:
            logging.error(f"❌ Error al consultar POSPAGO: {e}")
            mensaje = "❌ Ocurrió un error al consultar POSPAGO."

    return render_template(
        "buscar_ciclo.html",
        anios=anios_df.to_dict(orient="records"),
        ciclos=ciclos_df.to_dict(orient="records"),
        resultados=resultados,
        mensaje=mensaje
    )

# ==============================
# Ejecutar aplicación
# ==============================
if __name__ == "__main__":
    app.run(debug=True, port=5000)
