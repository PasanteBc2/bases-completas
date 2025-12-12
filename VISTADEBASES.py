import tkinter as tk  # Interfaz gráfica
from tkinter import messagebox  # Mensajes emergentes
import subprocess  # Ejecutar scripts
import os  # Manejo de rutas
import sys  # Manejo de sistema

# ======================================
# Diccionario de scripts
# ======================================
scripts_objetivo = {
    "Pospago": "pospago.py",
    "Prepago": "prepago.py",
    "Pyme": "pyme.py",

    # === MOVISTAR ===
    "Movistar_Migracion": "VIS-MIG.py",
    "Movistar_Tradicional": "VIS-TRAD.py",
    "Movistar_Digital": "VIS-DIG.py"
}

# ======================================
# Buscar script (con ruta fija de Movistar)
# ======================================
def buscar_script(nombre_script):

    movistar_path = r"C:\Users\pasante.ti2\Desktop\cargarBases-20250917T075622Z-1-001\Bases.Movi"

    # 1️⃣ Buscar primero en MOVISTAR
    for root, _, files in os.walk(movistar_path):
        if nombre_script in files:
            return os.path.join(root, nombre_script)

    # 2️⃣ Buscar en la carpeta normal del programa
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
        messagebox.showerror("❌ Error", f"No se encontró '{nombre_script}'.")
        return

    try:
        messagebox.showinfo("⏳ Ejecutando", f"Iniciando {nombre_script}...")
        subprocess.run([sys.executable, ruta], check=True)
        messagebox.showinfo("✅ Finalizado", f"{nombre_script} se ejecutó correctamente.")
    except subprocess.CalledProcessError as e:
        messagebox.showerror("⚠️ Error", f"Ocurrió un error ejecutando {nombre_script}\n\n{e}")

# ======================================
# Función volver al menú
# ======================================
def volver_menu():
    frame.place_forget()
    movistar_frame.place_forget()
    menu_frame.place(relx=0.5, rely=0.5, anchor="center", width=335, height=335)

# ======================================
# Ventana principal
# ======================================
ventana = tk.Tk()
ventana.title("🚀 Cargador de Bases")
ventana.geometry("440x420")
ventana.resizable(False, False)

canvas = tk.Canvas(ventana, width=440, height=420, highlightthickness=0)
canvas.pack(fill="both", expand=True)
for i in range(420):
    r = int(255 - (i / 4))
    g = int(255 - (i / 6))
    b = 197
    color = f"#{r:02x}{g:02x}{b:02x}"
    canvas.create_line(0, i, 440, i, fill=color)

# ======================================
# Menú principal
# ======================================
menu_frame = tk.Frame(ventana, bg="#FDFFDE")
menu_frame.place(relx=0.5, rely=0.5, anchor="center", width=335, height=335)

tk.Label(menu_frame, text="🚀 CARGADOR DE BASES", font=("Segoe UI Black", 15),
         fg="#AF3583", bg="#FDFFDE").pack(pady=(40, 10))

tk.Label(menu_frame, text="Seleccione proveedor:",
         font=("Segoe UI", 11), fg="#320773", bg="#FDFFDE").pack(pady=(0, 20))

tk.Button(menu_frame, text="Proveedor", font=("Segoe UI Semibold", 13),
          bg="#8CEBE6", fg="black", height=2, relief="flat",
          command=lambda: mostrar_proveedor()).pack(pady=10, fill="x", padx=40)

tk.Button(menu_frame, text="Movistar", font=("Segoe UI Semibold", 13),
          bg="#C59FE9", fg="black", height=2, relief="flat",
          command=lambda: mostrar_movistar()).pack(pady=10, fill="x", padx=40)

# ======================================
# Frame Proveedor
# ======================================
frame = tk.Frame(ventana, bg="#FDFFDE", bd=0, relief="flat", highlightthickness=0)

def mostrar_proveedor():
    menu_frame.place_forget()
    movistar_frame.place_forget()
    frame.place(relx=0.5, rely=0.5, anchor="center", width=335, height=335)

tk.Label(frame, text="🚀 CARGADOR DE BASES", font=("Segoe UI Black", 15),
         fg="#AF3583", bg="#FDFFDE").pack(pady=(20, 5))

tk.Label(frame, text="Seleccione el tipo de carga:",
         font=("Segoe UI", 10), fg="#320773", bg="#FDFFDE").pack(pady=(0, 15))

def crear_boton(texto, color, icono, accion):
    contenedor = tk.Frame(frame, bg="#FDFFDE")
    contenedor.pack(pady=8, fill="x", padx=25)
    boton = tk.Button(
        contenedor,
        text=f"{icono}  {texto}",
        font=("Segoe UI Semibold", 13),
        bg=color,
        fg="BLACK",
        relief="flat",
        height=2,
        command=accion
    )
    boton.pack(fill="x", padx=2, pady=2)

crear_boton("Ejecutar POSPAGO", "#ECFF8C", "📘", lambda: ejecutar_script("Pospago"))
crear_boton("Ejecutar PREPAGO", "#8CEBE6", "💳", lambda: ejecutar_script("Prepago"))
crear_boton("Ejecutar PYME", "#C59FE9", "🏢", lambda: ejecutar_script("Pyme"))

tk.Button(frame, text="🔙 Volver", font=("Segoe UI Semibold", 11),
          bg="#FFD4D4", fg="black",
          command=volver_menu).pack(pady=20)

# ======================================
# Frame MOVISTAR
# ======================================
movistar_frame = tk.Frame(ventana, bg="#FDFFDE")

def mostrar_movistar():
    frame.place_forget()
    menu_frame.place_forget()
    movistar_frame.place(relx=0.5, rely=0.5, anchor="center", width=335, height=335)

tk.Label(movistar_frame, text="MOVISTAR", font=("Segoe UI Black", 15),
         fg="#005BAB", bg="#FDFFDE").pack(pady=(20, 10))

tk.Label(movistar_frame, text="Seleccione tipo:",
         font=("Segoe UI", 11), fg="#320773", bg="#FDFFDE").pack(pady=(0, 20))

tk.Button(movistar_frame, text="Migración", font=("Segoe UI Semibold", 13),
          bg="#8CEBE6", fg="black", height=2, relief="flat",
          command=lambda: ejecutar_script("Movistar_Migracion")
          ).pack(pady=5, fill="x", padx=40)

tk.Button(movistar_frame, text="Tradicional", font=("Segoe UI Semibold", 13),
          bg="#ECFF8C", fg="black", height=2, relief="flat",
          command=lambda: ejecutar_script("Movistar_Tradicional")
          ).pack(pady=5, fill="x", padx=40)

tk.Button(movistar_frame, text="Digital", font=("Segoe UI Semibold", 13),
          bg="#C59FE9", fg="black", height=2, relief="flat",
          command=lambda: ejecutar_script("Movistar_Digital")
          ).pack(pady=5, fill="x", padx=40)

tk.Button(movistar_frame, text="🔙 Volver", font=("Segoe UI Semibold", 11),
          bg="#FFD4D4", fg="black",
          command=volver_menu).pack(pady=30)

ventana.mainloop()
