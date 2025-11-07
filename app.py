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
# Conexión a la base de datos
# ==============================
usuario = 'postgres'
contraseña = quote_plus('pasante')
host = 'localhost'
puerto = '5432'
base_datos = 'BAS'

try:
    engine = create_engine(
        f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}',
        connect_args={"options": "-c client_encoding=UTF8"}
    )
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logging.info("✅ Conexión exitosa a la base de datos 'BAS'.")
except Exception as e:
    logging.error(f"❌ Error al conectar: {e}")

# ==============================
# Página de inicio
# ==============================
@app.route('/')
def inicio():
    return render_template('inicio.html')

# ==============================
# Página de búsqueda
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

        # ==============================
        # Consulta SQL según tipo
        # ==============================
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
            df = pd.read_sql(query, engine, params=params)
        except Exception as e:
            logging.error(f"❌ Error en la consulta SQL: {e}")
            return render_template('index.html', tipo=tipo, mensaje="❌ Error al consultar la base de datos.")

        if df.empty:
            return render_template('index.html', tipo=tipo, mensaje="❌ No se encontraron registros.", valores_input=valores_input)

        # ==============================
        # Marcar duplicados y ordenar
        # ==============================
        df['duplicado'] = df.duplicated(subset=['celular'], keep=False)

        orden_columnas = ['celular']
        if 'año' in df.columns and 'mes' in df.columns:
            orden_columnas += ['año', 'mes']
        df = df.sort_values(by=orden_columnas, ascending=[True] * len(orden_columnas))

        resultados = df.to_dict(orient='records')
        return render_template('index.html', tipo=tipo, resultados=resultados, valores_input=valores_input)

    return render_template('index.html', tipo=tipo)

# ==============================
# Ejecutar aplicación
# ==============================
if __name__ == '__main__':
    app.run(debug=True, port=5000)
