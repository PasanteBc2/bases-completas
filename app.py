from flask import Flask, render_template, request
from sqlalchemy import create_engine, text
import pandas as pd
from urllib.parse import quote_plus

app = Flask(__name__)

# ==============================
# 🔗 Conexión a PostgreSQL
# ==============================
usuario = 'postgres'
contraseña = quote_plus('pasante')
host = 'localhost'
puerto = '5432'
base_datos = 'BASES'

engine = create_engine(f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_datos}')

# ==============================
# 🌐 Rutas Flask
# ==============================
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/buscar', methods=['POST'])
def buscar():
    valores_input = request.form.get('valores', '').strip()

    # Dividir valores ingresados por coma o salto de línea
    valores = [v.strip() for v in valores_input.replace('\n', ',').split(',') if v.strip()]

    if not valores:
        return render_template('index.html', mensaje="⚠️ Ingrese al menos un número o cédula.")
    if len(valores) > 5:
        return render_template('index.html', mensaje="⚠️ Solo se pueden consultar hasta 5 valores.")

    # Determinar tipo de búsqueda (celular o cédula)
    if all(len(v) == 10 and v.isdigit() for v in valores):
        campo = "celular"
    else:
        campo = "identificacion"

    # Crear placeholders dinámicos
    placeholders = ', '.join([f":val{i}" for i in range(len(valores))])

    # Consulta con DISTINCT ON para evitar duplicados
    query = text(f"""
        SELECT DISTINCT ON (identificacion)
               identificacion,
               celular,
               nombre_completo,
               origen
        FROM cliente_consolidado
        WHERE {campo} IN ({placeholders})
        ORDER BY identificacion, origen;
    """)

    # Pasar parámetros
    params = {f"val{i}": v for i, v in enumerate(valores)}

    # Ejecutar consulta
    df = pd.read_sql(query, engine, params=params)

    if df.empty:
        return render_template('index.html', mensaje="❌ No se encontraron registros.")
    else:
        resultados = df.to_dict(orient='records')
        return render_template('index.html', resultados=resultados)

if __name__ == '__main__':
    app.run(debug=True)
