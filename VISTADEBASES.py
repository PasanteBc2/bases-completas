import tkinter as tk
from tkinter import messagebox
import subprocess
import os
import sys

# ======================================
# Diccionario de scripts
# ======================================
scripts_objetivo = {
    "Pospago": "pospago.py",
    "Prepago": "prepago.py",
    "Pyme": "pyme.py"
}

# ======================================
# Buscar script en subcarpetas
# ======================================
def buscar_script(nombre_script):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    for root, _, files in os.walk(base_dir):
        if nombre_script in files:
            return os.path.join(root, nombre_script)
    return None

# ======================================
# Ejecutar script
# ======================================
def ejecutar_script(nombre_logico):
    nombre_script = scripts_objetivo[nombre_logico]
    ruta = buscar_script(nombre_script)

    if not ruta:
        messagebox.showerror("❌ Error", f"No se encontró '{nombre_script}' en:\n{os.path.dirname(os.path.abspath(__file__))}")
        return

    try:
        messagebox.showinfo("⏳ Ejecutando", f"Iniciando {nombre_script}...")
        subprocess.run([sys.executable, ruta], check=True)
        messagebox.showinfo("✅ Finalizado", f"{nombre_script} se ejecutó correctamente.")
    except subprocess.CalledProcessError as e:
        messagebox.showerror("⚠️ Error", f"Ocurrió un error ejecutando {nombre_script}\n\n{e}")

# ======================================
# Ventana principal
# ======================================
ventana = tk.Tk()
ventana.title("🚀 Cargador de Bases")
ventana.geometry("440x420")
ventana.resizable(False, False)

# Fondo degradado
canvas = tk.Canvas(ventana, width=440, height=420, highlightthickness=0)
canvas.pack(fill="both", expand=True)

# Crear degradado suave
for i in range(420):
    r = int(255 - (i / 4))
    g = int(255 - (i / 6))
    b = 197
    color = f"#{r:02x}{g:02x}{b:02x}"
    canvas.create_line(0, i, 440, i, fill=color)

# ======================================
# Frame central con sombra
# ======================================
shadow = tk.Frame(ventana, bg="#6B0B7B")
shadow.place(relx=0.5, rely=0.5, anchor="center", width=340, height=340)

frame = tk.Frame(ventana, bg="#FDFFDE", bd=0, relief="flat", highlightthickness=0)
frame.place(relx=0.5, rely=0.5, anchor="center", width=335, height=335)

# Bordes redondeados
frame.config(highlightbackground="#C04BDB", highlightthickness=1)

# ======================================
# Título con diseño
# ======================================
titulo = tk.Label(
    frame,
    text="🚀 CARGADOR DE BASES",
    font=("Segoe UI Black", 15),
    fg="#AF3583",
    bg="#FDFFDE"
)
titulo.pack(pady=(20, 5))

subtitulo = tk.Label(
    frame,
    text="Seleccione el tipo de carga:",
    font=("Segoe UI", 10),
    fg="#320773",
    bg="#FDFFDE"
)
subtitulo.pack(pady=(0, 15))

# ======================================
# Estilo de botones con íconos
# ======================================
def crear_boton(texto, color, icono, accion):
    contenedor = tk.Frame(frame, bg="#000000")
    contenedor.pack(pady=8, fill="x", padx=25)

    boton = tk.Button(
        contenedor,
        text=f"{icono}  {texto}",
        font=("Segoe UI Semibold", 13),
        bg=color,
        fg="BLACK",
        activebackground="#2C2C2C",
        activeforeground="white",
        relief="flat",
        bd=0,
        height=2,
        cursor="hand2",
        command=accion
    )
    boton.pack(fill="x", padx=2, pady=2)

    def on_enter(e): boton.config(bg="#D4C4B6")
    def on_leave(e): boton.config(bg=color)
    boton.bind("<Enter>", on_enter)
    boton.bind("<Leave>", on_leave)

# Botones
crear_boton("Ejecutar POSPAGO", "#ECFF8C", "📘", lambda: ejecutar_script("Pospago"))
crear_boton("Ejecutar PREPAGO", "#8CEBE6", "💳", lambda: ejecutar_script("Prepago"))
crear_boton("Ejecutar PYME", "#C59FE9", "🏢", lambda: ejecutar_script("Pyme"))

# ======================================
# Línea decorativa
# ======================================
canvas_line = tk.Canvas(frame, width=250, height=2, bg="#FFFFFF", highlightthickness=0)
canvas_line.create_line(0, 2, 250, 2, fill="#FFFA76", width=2)
canvas_line.pack(pady=20)

# ======================================
# Pie de página
# ======================================
footer = tk.Label(
    frame,
    text="© 2025 Departamento TI | Sistema Automático",
    font=("Segoe UI", 9),
    bg="#FFDEDE",
    fg="#9E9E9E"
)
footer.pack(side="bottom", pady=8)

ventana.mainloop()
