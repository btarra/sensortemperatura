import os
import csv
import math
import time
import sqlite3
import threading
from collections import deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import paho.mqtt.client as mqtt
import streamlit as st

# ==========================================
# 1. CONFIGURACIÓN GENERAL
# ==========================================

BROKER_IP = "broker.hivemq.com"
BROKER_PORT = 1883
TOPIC = "esp32/temperatura/tarjeta1"

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
ARCHIVO_RESPALDO = os.path.join(BASE_DIR, "historial_mqtt.csv")
ARCHIVO_DB = os.path.join(BASE_DIR, "historial_mqtt.db")
ZONA_HORARIA = ZoneInfo("America/Bogota")

# Ventana de puntos visibles en modo "En vivo"
MAX_MOSTRAR_VIVO = 30
# Máximo de puntos dibujados simultáneamente en bloques extensos (para fluidez visual)
MAX_PUNTOS_BLOQUE = 360
INTERVALO_ACTUALIZACION = "1s"

# Límite máximo en RAM para el día actual (1 día a 2s = 43,200 lecturas)
MAX_RAM_DIA = 60_000

# Tiempo máximo sin recibir paquetes para considerar la ESP32 desconectada
TIMEOUT_ESP32_SEGUNDOS = 6


st.set_page_config(
    page_title="Visualización de Variables",
    page_icon="🌡️",
    layout="wide",
)

# Estilo visual para los nodos clicables de división temporal
st.markdown(
    """
    <style>
    div[data-testid="stRadio"] > div[role="radiogroup"] {
        display: flex;
        flex-wrap: wrap;
        justify-content: flex-start;
        align-items: center;
        gap: 0.45rem;
        max-height: 165px;
        overflow-y: auto;
        padding: 0.75rem 1rem;
        background-color: rgba(128, 128, 128, 0.08);
        border: 1px solid rgba(128, 128, 128, 0.22);
        border-radius: 10px;
    }
    div[data-testid="stRadio"] > div[role="radiogroup"] label {
        background-color: rgba(255, 255, 255, 0.04);
        padding: 0.35rem 0.7rem;
        border-radius: 20px;
        border: 1px solid rgba(128, 128, 128, 0.25);
        cursor: pointer;
        transition: all 0.15s ease-in-out;
    }
    div[data-testid="stRadio"] > div[role="radiogroup"] label:hover {
        border-color: #1f77b4;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ==========================================
# 2. UTILIDADES DE FECHA Y HORA
# ==========================================

def obtener_ahora():
    return datetime.now(ZONA_HORARIA)


def formato_hora(fecha_hora):
    return fecha_hora.strftime("%I:%M:%S%p").lstrip("0").lower()


def construir_puntos_division(df_o_lista, es_hoy=True, modo_escala="Automática", max_puntos_auto=9):
    """
    Genera puntos específicos de división temporal según la escala elegida.
    Soporta jornadas con 1 sola lectura sin dejar bloques vacíos.
    """
    if isinstance(df_o_lista, list):
        if not df_o_lista:
            return [], "10 segundos", {}
        t_min = pd.Timestamp(df_o_lista[0]["Momento"])
        t_max = pd.Timestamp(df_o_lista[-1]["Momento"])
    else:
        if df_o_lista.empty:
            return [], "10 segundos", {}
        t_min = pd.Timestamp(df_o_lista["Momento"].iloc[0])
        t_max = pd.Timestamp(df_o_lista["Momento"].iloc[-1])

    duracion_seg = max(1.0, (t_max - t_min).total_seconds())

    if modo_escala == "Horas":
        freq_str, paso_td, etiqueta_escala, fmt_hora = (
            "1h",
            timedelta(hours=1),
            "1 hora",
            "%I:%M%p",
        )
    elif modo_escala == "Minutos":
        freq_str, paso_td, etiqueta_escala, fmt_hora = (
            "10min",
            timedelta(minutes=10),
            "10 minutos",
            "%I:%M%p",
        )
    else:
        escalas = [
            ("10s", timedelta(seconds=10), "10 segundos", "%I:%M:%S%p"),
            ("15s", timedelta(seconds=15), "15 segundos", "%I:%M:%S%p"),
            ("30s", timedelta(seconds=30), "30 segundos", "%I:%M:%S%p"),
            ("1min", timedelta(minutes=1), "1 minuto", "%I:%M%p"),
            ("2min", timedelta(minutes=2), "2 minutos", "%I:%M%p"),
            ("5min", timedelta(minutes=5), "5 minutos", "%I:%M%p"),
            ("10min", timedelta(minutes=10), "10 minutos", "%I:%M%p"),
            ("15min", timedelta(minutes=15), "15 minutos", "%I:%M%p"),
            ("30min", timedelta(minutes=30), "30 minutos", "%I:%M%p"),
            ("1h", timedelta(hours=1), "1 hora", "%I:%M%p"),
            ("2h", timedelta(hours=2), "2 horas", "%I:%M%p"),
            ("3h", timedelta(hours=3), "3 horas", "%I:%M%p"),
            ("4h", timedelta(hours=4), "4 horas", "%I:%M%p"),
        ]

        elegida = escalas[-1]
        for f_s, p_td, e_esc, fmt_h in escalas:
            if (duracion_seg / p_td.total_seconds()) <= max_puntos_auto:
                elegida = (f_s, p_td, e_esc, fmt_h)
                break
        freq_str, paso_td, etiqueta_escala, fmt_hora = elegida

    inicio_bloque = t_min.floor(freq_str)
    fin_bloque = t_max.floor(freq_str)
    marcas_dt = pd.date_range(start=inicio_bloque, end=fin_bloque, freq=freq_str)

    if modo_escala == "Automática" and len(marcas_dt) > max_puntos_auto:
        marcas_dt = marcas_dt[-max_puntos_auto:]

    mapa_bloques = {}
    opciones = []
    for dt_bloque in marcas_dt:
        etiqueta = dt_bloque.strftime(fmt_hora).lstrip("0").lower()
        if etiqueta not in mapa_bloques:
            opciones.append(etiqueta)
            mapa_bloques[etiqueta] = (
                dt_bloque.to_pydatetime(),
                (dt_bloque + paso_td).to_pydatetime(),
                fmt_hora,
            )

    etiqueta_final = "🔴 En vivo" if es_hoy else "⏭ Últimas lecturas"
    opciones.append(etiqueta_final)
    return opciones, etiqueta_escala, mapa_bloques


# ==========================================
# 3. RECOLECTOR MQTT CON MEMORIA PERSISTENTE MULTI-DÍA
# ==========================================

class RecolectorMQTT:
    def __init__(self):
        self.lock = threading.Lock()
        self.fecha_actual = obtener_ahora().strftime("%Y-%m-%d")

        # Buffer circular en RAM solo para el día en curso
        self.historial_hoy = deque(maxlen=MAX_RAM_DIA)

        # Estadísticas acumuladas en O(1) para el día actual
        self.count_hoy = 0
        self.suma_hoy = 0.0
        self.min_hoy = None
        self.max_hoy = None

        # Índice persistente de días anteriores y conteo por fecha
        self.total_historico = 0
        self.conteo_por_fecha = {self.fecha_actual: 0}

        # Estado de conexión Broker MQTT y ESP32
        self.mqtt_connected = False
        self.mqtt_error = None
        self.ultimo_mensaje_ts = None
        self.ultima_hora_esp32 = None

        # Conexión SQLite persistente en disco
        self.db_conn = sqlite3.connect(ARCHIVO_DB, check_same_thread=False)
        self._inicializar_db_y_migrar_csv()
        self._cargar_memoria_historica()
        self._iniciar_cliente()

    def _reiniciar_estadisticas_dia(self, nueva_fecha):
        self.fecha_actual = nueva_fecha
        self.historial_hoy.clear()
        self.count_hoy = 0
        self.suma_hoy = 0.0
        self.min_hoy = None
        self.max_hoy = None
        self.conteo_por_fecha.setdefault(nueva_fecha, 0)

    def _registrar_en_estadisticas_hoy(self, temp):
        self.count_hoy += 1
        self.suma_hoy += temp
        if self.min_hoy is None or temp < self.min_hoy:
            self.min_hoy = temp
        if self.max_hoy is None or temp > self.max_hoy:
            self.max_hoy = temp

    def _inicializar_db_y_migrar_csv(self):
        """Crea la tabla SQLite e importa registros previos de historial_mqtt.csv si existen."""
        with self.lock:
            cur = self.db_conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS lecturas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fecha TEXT NOT NULL,
                    hora TEXT NOT NULL,
                    temperatura REAL NOT NULL,
                    momento_iso TEXT NOT NULL
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_lecturas_fecha ON lecturas(fecha)")
            self.db_conn.commit()

            cur.execute("SELECT COUNT(*) FROM lecturas")
            filas_db = cur.fetchone()[0]

            if filas_db == 0 and os.path.exists(ARCHIVO_RESPALDO):
                try:
                    for chunk in pd.read_csv(
                        ARCHIVO_RESPALDO,
                        chunksize=100_000,
                        encoding="utf-8-sig",
                        on_bad_lines="skip",
                    ):
                        if chunk.empty or len(chunk.columns) < 3:
                            continue

                        chunk = chunk.iloc[:, :3].copy()
                        chunk.columns = ["Fecha", "Hora", "Temperatura (°C)"]
                        chunk["Fecha"] = chunk["Fecha"].astype(str).str.strip()
                        chunk["Hora"] = chunk["Hora"].astype(str).str.strip()
                        chunk["Temperatura (°C)"] = pd.to_numeric(
                            chunk["Temperatura (°C)"], errors="coerce"
                        )
                        chunk = chunk.dropna(subset=["Fecha", "Hora", "Temperatura (°C)"])

                        if chunk.empty:
                            continue

                        momentos = pd.to_datetime(
                            chunk["Fecha"] + " " + chunk["Hora"],
                            format="mixed",
                            errors="coerce",
                        )

                        lote_sql = []
                        for idx, row in chunk.iterrows():
                            ts = momentos.loc[idx]
                            if pd.isna(ts):
                                continue
                            dt_bogota = ts.to_pydatetime().replace(tzinfo=ZONA_HORARIA)
                            lote_sql.append(
                                (
                                    row["Fecha"],
                                    row["Hora"],
                                    float(row["Temperatura (°C)"]),
                                    dt_bogota.isoformat(),
                                )
                            )

                        if lote_sql:
                            cur.executemany(
                                "INSERT INTO lecturas (fecha, hora, temperatura, momento_iso) VALUES (?, ?, ?, ?)",
                                lote_sql,
                            )
                            self.db_conn.commit()
                except Exception as e:
                    self.mqtt_error = f"Error al migrar CSV histórico a SQLite: {e}"

    def _cargar_memoria_historica(self):
        """Indexa todas las fechas almacenadas (incluso con 1 sola lectura) y carga el día actual."""
        with self.lock:
            try:
                cur = self.db_conn.cursor()
                cur.execute("SELECT fecha, COUNT(*) FROM lecturas GROUP BY fecha")
                resumen_fechas = cur.fetchall()

                self.conteo_por_fecha = {self.fecha_actual: 0}
                self.total_historico = 0
                for fecha_db, conteo in resumen_fechas:
                    self.conteo_por_fecha[fecha_db] = int(conteo)
                    self.total_historico += int(conteo)

                cur.execute(
                    """
                    SELECT fecha, hora, temperatura, momento_iso
                    FROM lecturas
                    WHERE fecha = ?
                    ORDER BY id ASC
                    """,
                    (self.fecha_actual,),
                )
                filas_hoy = cur.fetchall()

                for fecha_r, hora_r, temp_r, momento_iso_r in filas_hoy:
                    dt_bogota = datetime.fromisoformat(momento_iso_r)
                    if dt_bogota.tzinfo is None:
                        dt_bogota = dt_bogota.replace(tzinfo=ZONA_HORARIA)
                    temp_val = float(temp_r)

                    self.historial_hoy.append(
                        {
                            "Fecha": fecha_r,
                            "Hora": hora_r,
                            "Temperatura (°C)": temp_val,
                            "Momento": dt_bogota,
                        }
                    )
                    self._registrar_en_estadisticas_hoy(temp_val)

            except Exception as e:
                self.mqtt_error = f"Error al cargar memoria histórica: {e}"

    def _guardar_en_disco(self, registro):
        """Persiste inmediatamente en SQLite y CSV para no perder ni una lectura aislada."""
        try:
            cur = self.db_conn.cursor()
            cur.execute(
                "INSERT INTO lecturas (fecha, hora, temperatura, momento_iso) VALUES (?, ?, ?, ?)",
                (
                    registro["Fecha"],
                    registro["Hora"],
                    float(registro["Temperatura (°C)"]),
                    registro["Momento"].isoformat(),
                ),
            )
            self.db_conn.commit()
        except Exception as e:
            self.mqtt_error = f"Error al guardar en SQLite: {e}"

        archivo_nuevo = not os.path.exists(ARCHIVO_RESPALDO)
        try:
            with open(ARCHIVO_RESPALDO, mode="a", newline="", encoding="utf-8-sig") as f:
                escritor = csv.writer(f)
                if archivo_nuevo:
                    escritor.writerow(["Fecha", "Hora", "Temperatura (°C)"])
                escritor.writerow(
                    [
                        registro["Fecha"],
                        registro["Hora"],
                        f"{registro['Temperatura (°C)']:.2f}",
                    ]
                )
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            pass

    def obtener_lecturas_fecha(self, fecha_objetivo):
        """Recupera de SQLite todas las lecturas de un día histórico específico."""
        with self.lock:
            cur = self.db_conn.cursor()
            cur.execute(
                """
                SELECT fecha, hora, temperatura, momento_iso
                FROM lecturas
                WHERE fecha = ?
                ORDER BY id ASC
                """,
                (fecha_objetivo,),
            )
            filas = cur.fetchall()

        if not filas:
            return pd.DataFrame(columns=["Fecha", "Hora", "Temperatura (°C)", "Momento"])

        registros = []
        for fecha_r, hora_r, temp_r, momento_iso_r in filas:
            dt_bogota = datetime.fromisoformat(momento_iso_r)
            if dt_bogota.tzinfo is None:
                dt_bogota = dt_bogota.replace(tzinfo=ZONA_HORARIA)
            registros.append(
                {
                    "Fecha": fecha_r,
                    "Hora": hora_r,
                    "Temperatura (°C)": float(temp_r),
                    "Momento": dt_bogota,
                }
            )

        return pd.DataFrame(registros)

    def _iniciar_cliente(self):
        def on_connect(client, userdata, flags, reason_code, properties):
            with self.lock:
                if reason_code == 0:
                    client.subscribe(TOPIC)
                    self.mqtt_connected = True
                    self.mqtt_error = None
                else:
                    self.mqtt_connected = False

        def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
            with self.lock:
                self.mqtt_connected = False

        def on_message(client, userdata, msg):
            try:
                temperatura = float(msg.payload.decode("utf-8").strip())

                if not math.isfinite(temperatura):
                    return
                if not -55 <= temperatura <= 125:
                    return

                ahora = obtener_ahora()
                fecha_str = ahora.strftime("%Y-%m-%d")
                hora_str = formato_hora(ahora)

                registro = {
                    "Fecha": fecha_str,
                    "Hora": hora_str,
                    "Temperatura (°C)": temperatura,
                    "Momento": ahora,
                }

                with self.lock:
                    if fecha_str != self.fecha_actual:
                        self._reiniciar_estadisticas_dia(fecha_str)

                    self.historial_hoy.append(registro)
                    self._registrar_en_estadisticas_hoy(temperatura)
                    self.total_historico += 1
                    self.conteo_por_fecha[fecha_str] = self.conteo_por_fecha.get(fecha_str, 0) + 1

                    self.ultimo_mensaje_ts = time.monotonic()
                    self.ultima_hora_esp32 = hora_str

                    self._guardar_en_disco(registro)

            except (ValueError, UnicodeDecodeError):
                pass

        try:
            client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            client.on_connect = on_connect
            client.on_disconnect = on_disconnect
            client.on_message = on_message
            client.connect(BROKER_IP, BROKER_PORT, 60)
            client.loop_start()
            self.client = client
        except Exception as e:
            self.mqtt_error = str(e)


@st.cache_resource
def obtener_recolector():
    return RecolectorMQTT()


recolector = obtener_recolector()


# ==========================================
# 4. CARGA CACHEADA DE DÍAS ANTERIORES Y CSV
# ==========================================

@st.cache_data(ttl=30, show_spinner="Cargando registros del día seleccionado...")
def cargar_datos_dia_historico(fecha_objetivo, conteo_esperado):
    """Consulta la memoria persistente para el día elegido (se invalida si cambia el conteo)."""
    return recolector.obtener_lecturas_fecha(fecha_objetivo)


@st.cache_data(ttl=30)
def obtener_csv_dia_historico(fecha_objetivo, conteo_esperado):
    """Genera los bytes CSV de un día anterior cacheado para descarga instantánea."""
    df_dia = cargar_datos_dia_historico(fecha_objetivo, conteo_esperado)
    if df_dia.empty:
        return b""
    return df_dia[["Fecha", "Hora", "Temperatura (°C)"]].to_csv(index=False).encode("utf-8-sig")


@st.cache_data(ttl=5)
def obtener_csv_hoy_cacheado(fecha_hoy, total_hoy):
    """Genera los bytes CSV del día actual cada 5s para no sobrecargar el ciclo de 1s."""
    with recolector.lock:
        datos = list(recolector.historial_hoy)
    if not datos:
        return b""
    df_hoy = pd.DataFrame(
        [(r["Fecha"], r["Hora"], r["Temperatura (°C)"]) for r in datos],
        columns=["Fecha", "Hora", "Temperatura (°C)"],
    )
    return df_hoy.to_csv(index=False).encode("utf-8-sig")


# ==========================================
# 5. ESTADO DE NAVEGACIÓN Y PAUSA DE LA INTERFAZ
# ==========================================

if "fecha_seleccionada" not in st.session_state:
    st.session_state.fecha_seleccionada = obtener_ahora().strftime("%Y-%m-%d")

if "viendo_dia_actual" not in st.session_state:
    st.session_state.viendo_dia_actual = True

if "escala_seleccionada" not in st.session_state:
    st.session_state.escala_seleccionada = "Automática"

if "grafica_pausada" not in st.session_state:
    st.session_state.grafica_pausada = False

if "snapshot_grafica" not in st.session_state:
    st.session_state.snapshot_grafica = pd.DataFrame()

if "snapshot_tabla" not in st.session_state:
    st.session_state.snapshot_tabla = pd.DataFrame()

if "snapshot_titulo_tabla" not in st.session_state:
    st.session_state.snapshot_titulo_tabla = ""

if "ultimo_punto_radio" not in st.session_state:
    st.session_state.ultimo_punto_radio = None


# ==========================================
# 6. ENCABEZADO PRINCIPAL
# ==========================================

st.title("🌡️ Dashboard: Monitoreo Remoto Wi-Fi")

st.markdown(
    "Visualización escalable de las lecturas del sensor **DS18B20**, "
    "transmitidas mediante una **ESP32** por MQTT con almacenamiento histórico "
    "persistente por días."
)

st.divider()


# ==========================================
# 7. DASHBOARD EN TIEMPO REAL E HISTÓRICO
# ==========================================

@st.fragment(run_every=INTERVALO_ACTUALIZACION)
def dashboard():

    fecha_hoy = obtener_ahora().strftime("%Y-%m-%d")

    # Leer estado instantáneo en O(1) del recolector
    with recolector.lock:
        mqtt_conectado = recolector.mqtt_connected
        mqtt_error = recolector.mqtt_error
        ultimo_ts = recolector.ultimo_mensaje_ts
        ultima_hora_esp32 = recolector.ultima_hora_esp32
        total_global = recolector.total_historico
        conteos_fechas = dict(recolector.conteo_por_fecha)
        conteos_fechas.setdefault(fecha_hoy, 0)

        count_hoy = recolector.count_hoy
        suma_hoy = recolector.suma_hoy
        min_hoy = recolector.min_hoy
        max_hoy = recolector.max_hoy

        lista_hoy = list(recolector.historial_hoy)

    fechas_disp = sorted(conteos_fechas.keys(), reverse=True)

    # Calcular estado real de la ESP32
    if ultimo_ts is not None:
        segundos_sin_datos = time.monotonic() - ultimo_ts
        esp32_conectada = segundos_sin_datos <= TIMEOUT_ESP32_SEGUNDOS
    else:
        segundos_sin_datos = None
        esp32_conectada = False

    # ==========================================
    # 8. BLOQUES DE JORNADA, ESCALA Y EXPORTACIÓN CSV
    # ==========================================

    col_fecha, col_escala, col_export = st.columns([2, 2, 3])

    # Bloque 1: Selección de jornada (con salto automático al cruzar medianoche)
    with col_fecha:
        st.subheader("📅 Selección de jornada")

        if st.session_state.viendo_dia_actual and st.session_state.fecha_seleccionada != fecha_hoy:
            st.session_state.fecha_seleccionada = fecha_hoy
            st.session_state.grafica_pausada = False
            st.session_state.snapshot_grafica = pd.DataFrame()
            st.session_state.snapshot_tabla = pd.DataFrame()
            st.session_state.snapshot_titulo_tabla = ""
            st.session_state.ultimo_punto_radio = None

        if st.session_state.fecha_seleccionada not in fechas_disp:
            st.session_state.fecha_seleccionada = fecha_hoy
            st.session_state.viendo_dia_actual = True

        def formatear_opcion_dia(f):
            n_reg = conteos_fechas.get(f, 0)
            sufijo_reg = "1 lectura única" if n_reg == 1 else f"{n_reg:,} lecturas"
            if f == fecha_hoy:
                return f"{f} (Hoy — {sufijo_reg})"
            return f"{f} (Historial — {sufijo_reg})"

        fecha_elegida = st.selectbox(
            "Día a visualizar y exportar:",
            options=fechas_disp,
            index=fechas_disp.index(st.session_state.fecha_seleccionada),
            format_func=formatear_opcion_dia,
            key="selector_dia_widget",
        )

        if fecha_elegida != st.session_state.fecha_seleccionada:
            st.session_state.fecha_seleccionada = fecha_elegida
            st.session_state.viendo_dia_actual = (fecha_elegida == fecha_hoy)
            st.session_state.grafica_pausada = False
            st.session_state.snapshot_grafica = pd.DataFrame()
            st.session_state.snapshot_tabla = pd.DataFrame()
            st.session_state.snapshot_titulo_tabla = ""
            st.session_state.ultimo_punto_radio = None
            st.rerun()

    # Bloque 2: Selección de escala de división (Horas / Minutos / Automática)
    with col_escala:
        st.subheader("⏱️ Escala de división")

        opciones_escala = ["Automática", "Horas", "Minutos"]
        etiquetas_escala = {
            "Automática": "Automática (Seg. / Min. / Horas)",
            "Horas": "Horas (Bloques de 1 hora)",
            "Minutos": "Minutos (Bloques de 10 min)",
        }

        escala_elegida = st.selectbox(
            "Granularidad de puntos en gráfica:",
            options=opciones_escala,
            index=opciones_escala.index(st.session_state.escala_seleccionada),
            format_func=lambda e: etiquetas_escala[e],
            key="selector_escala_widget",
        )

        if escala_elegida != st.session_state.escala_seleccionada:
            st.session_state.escala_seleccionada = escala_elegida
            st.session_state.grafica_pausada = False
            st.session_state.snapshot_grafica = pd.DataFrame()
            st.session_state.snapshot_tabla = pd.DataFrame()
            st.session_state.snapshot_titulo_tabla = ""
            st.session_state.ultimo_punto_radio = None
            radio_key_reset = f"puntos_tiempo_{fecha_elegida}_{escala_elegida}"
            if radio_key_reset in st.session_state:
                del st.session_state[radio_key_reset]
            st.rerun()

    es_hoy = (fecha_elegida == fecha_hoy)
    conteo_esperado_dia = conteos_fechas.get(fecha_elegida, 0)

    # Preparar datos del día elegido
    if es_hoy:
        total_dia = len(lista_hoy)
        if total_dia > 0:
            temp_actual = lista_hoy[-1]["Temperatura (°C)"]
            temp_min = min_hoy
            temp_max = max_hoy
            temp_prom = suma_hoy / count_hoy
        else:
            temp_actual = temp_min = temp_max = temp_prom = None
        df_dia_historico = None
    else:
        df_dia_historico = cargar_datos_dia_historico(fecha_elegida, conteo_esperado_dia)
        total_dia = len(df_dia_historico)
        if total_dia > 0:
            temp_actual = float(df_dia_historico["Temperatura (°C)"].iloc[-1])
            temp_min = float(df_dia_historico["Temperatura (°C)"].min())
            temp_max = float(df_dia_historico["Temperatura (°C)"].max())
            temp_prom = float(df_dia_historico["Temperatura (°C)"].mean())
        else:
            temp_actual = temp_min = temp_max = temp_prom = None

    # Bloque 3: Exportación CSV (General + Día seleccionado)
    with col_export:
        st.subheader("📥 Exportación de datos CSV")
        col_csv_general, col_csv_dia = st.columns(2)

        with col_csv_general:
            if os.path.exists(ARCHIVO_RESPALDO) and total_global > 0:
                with open(ARCHIVO_RESPALDO, "rb") as archivo_csv:
                    st.download_button(
                        label=f"🌐 CSV General ({total_global:,} reg.)",
                        data=archivo_csv,
                        file_name="historial_general_temperaturas.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key="descargar_csv_general",
                    )
            else:
                st.button(
                    "🌐 CSV General (0 reg.)",
                    disabled=True,
                    use_container_width=True,
                    key="descargar_csv_general_vacio",
                )

        with col_csv_dia:
            if total_dia > 0:
                csv_dia_bytes = (
                    obtener_csv_hoy_cacheado(fecha_hoy, total_dia)
                    if es_hoy
                    else obtener_csv_dia_historico(fecha_elegida, total_dia)
                )
                st.download_button(
                    label=f"📅 CSV del {fecha_elegida} ({total_dia:,} reg.)",
                    data=csv_dia_bytes,
                    file_name=f"historial_temperaturas_{fecha_elegida}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key=f"descargar_csv_dia_{fecha_elegida}",
                )
            else:
                st.button(
                    f"📅 CSV del {fecha_elegida} (0 reg.)",
                    disabled=True,
                    use_container_width=True,
                    key="descargar_csv_dia_vacio",
                )

        dias_con_datos = sum(1 for c in conteos_fechas.values() if c > 0)
        st.caption(
            f"Memoria global: {total_global:,} lecturas en {dias_con_datos} día(s) con registros | "
            f"Jornada ({fecha_elegida}): {total_dia:,} lectura(s)."
        )

    # ==========================================
    # 9. INDICADORES DE TEMPERATURA Y CONEXIÓN
    # ==========================================

    titulo_kpi = "📊 Indicadores de Hoy (En vivo)" if es_hoy else f"📊 Resumen del {fecha_elegida}"
    st.subheader(titulo_kpi)

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    if temp_actual is not None:
        etiqueta_ultima = "Temperatura Actual" if es_hoy else ("Lectura Única" if total_dia == 1 else "Última del Día")
        col1.metric(etiqueta_ultima, f"{temp_actual:.2f} °C")
        col2.metric("Mínima del Día", f"{temp_min:.2f} °C")
        col3.metric("Máxima del Día", f"{temp_max:.2f} °C")
        col4.metric("Promedio del Día", f"{temp_prom:.2f} °C")
    else:
        col1.metric("Temperatura Actual", "-- °C")
        col2.metric("Mínima del Día", "-- °C")
        col3.metric("Máxima del Día", "-- °C")
        col4.metric("Promedio del Día", "-- °C")

    if esp32_conectada:
        col5.metric(
            "ESP32 (Sensor)",
            "Conectada 🟢",
            delta=f"Último: {ultima_hora_esp32}",
            delta_color="normal",
        )
    else:
        detalle_esp = (
            f"Sin señal ({int(segundos_sin_datos)}s)"
            if segundos_sin_datos is not None
            else "Esperando datos..."
        )
        col5.metric(
            "ESP32 (Sensor)",
            "Desconectada 🔴",
            delta=detalle_esp,
            delta_color="inverse",
        )

    col6.metric(
        "Broker MQTT",
        "Conectado 🟢" if mqtt_conectado else "Desconectado 🔴",
        delta=f"{total_dia:,} lectura(s) hoy" if es_hoy else f"{total_dia:,} lectura(s) guardadas",
        delta_color="off",
    )

    # ==========================================
    # 10. GRÁFICA CON PUNTOS DE DIVISIÓN Y SOPORTE DE LECTURA ÚNICA
    # ==========================================

    st.subheader("📈 Comportamiento térmico")

    if total_dia > 0:
        datos_fuente = lista_hoy if es_hoy else df_dia_historico
        opciones_puntos, etiqueta_paso, mapa_bloques = construir_puntos_division(
            datos_fuente,
            es_hoy=es_hoy,
            modo_escala=escala_elegida,
        )

        radio_key = f"puntos_tiempo_{fecha_elegida}_{escala_elegida}"
        opcion_final = opciones_puntos[-1]

        if radio_key not in st.session_state or st.session_state[radio_key] not in opciones_puntos:
            st.session_state[radio_key] = opcion_final

        modo_texto_escala = (
            f"escala automática cada **{etiqueta_paso}**"
            if escala_elegida == "Automática"
            else f"escala fijada en **{escala_elegida}** (cada **{etiqueta_paso}**)"
        )

        punto_seleccionado = st.radio(
            f"Selecciona un punto de división ({modo_texto_escala} — último punto: **{opcion_final}**):",
            options=opciones_puntos,
            horizontal=True,
            key=radio_key,
        )

        if st.session_state.ultimo_punto_radio != punto_seleccionado:
            st.session_state.ultimo_punto_radio = punto_seleccionado
            if punto_seleccionado != opcion_final:
                st.session_state.grafica_pausada = False
                st.session_state.snapshot_grafica = pd.DataFrame()
                st.session_state.snapshot_tabla = pd.DataFrame()

        # Botón de Pausar / Reanudar gráfica y tabla + banner de estado
        col_btn_pausa, col_estado_graf = st.columns([1, 4])

        with col_btn_pausa:
            if es_hoy and punto_seleccionado == opcion_final:
                if st.session_state.grafica_pausada:
                    if st.button("▶ Reanudar gráfica", use_container_width=True, key="btn_reanudar_graf"):
                        st.session_state.grafica_pausada = False
                        st.rerun()
                else:
                    if st.button("⏸ Pausar gráfica", use_container_width=True, key="btn_pausar_graf"):
                        st.session_state.grafica_pausada = True
                        st.rerun()
            else:
                st.button(
                    "⏸ Pausar gráfica",
                    disabled=True,
                    use_container_width=True,
                    key="btn_pausar_graf_disabled",
                )

        # Filtrar datos para la gráfica y acoplar la tabla
        if punto_seleccionado == opcion_final:
            if es_hoy:
                if (
                    st.session_state.grafica_pausada
                    and not st.session_state.snapshot_grafica.empty
                    and not st.session_state.snapshot_tabla.empty
                ):
                    df_grafica = st.session_state.snapshot_grafica.copy()
                    df_tabla = st.session_state.snapshot_tabla.copy()
                    titulo_tabla = st.session_state.snapshot_titulo_tabla
                else:
                    df_grafica = pd.DataFrame(lista_hoy[-MAX_MOSTRAR_VIVO:])
                    st.session_state.snapshot_grafica = df_grafica.copy()

                    df_tabla = pd.DataFrame(
                        [(r["Fecha"], r["Hora"], r["Temperatura (°C)"]) for r in reversed(lista_hoy)],
                        columns=["Fecha", "Hora", "Temperatura (°C)"],
                    )
                    titulo_tabla = f"📋 Todas las lecturas del día ({fecha_elegida}) — {len(df_tabla):,} registro(s)"
                    st.session_state.snapshot_tabla = df_tabla.copy()
                    st.session_state.snapshot_titulo_tabla = titulo_tabla
            else:
                df_grafica = df_dia_historico.tail(MAX_MOSTRAR_VIVO).copy()
                df_tabla = df_dia_historico[["Fecha", "Hora", "Temperatura (°C)"]].iloc[::-1].reset_index(drop=True)
                titulo_tabla = f"📋 Todas las lecturas del día ({fecha_elegida}) — {len(df_tabla):,} registro(s)"

            with col_estado_graf:
                if es_hoy and st.session_state.grafica_pausada:
                    st.warning(
                        "⏸ Gráfica y tabla pausadas para inspección. Las nuevas lecturas siguen guardándose en segundo plano."
                    )
                elif total_dia == 1:
                    hora_unica = df_grafica["Hora"].iloc[0]
                    temp_unica = df_grafica["Temperatura (°C)"].iloc[0]
                    st.info(
                        f"📌 Jornada con **1 única lectura registrada** a las **{hora_unica}** "
                        f"(**{temp_unica:.2f} °C**)."
                    )
                elif es_hoy:
                    st.success(
                        f"Mostrando las últimas **{len(df_grafica)}** lecturas en tiempo real "
                        f"({modo_texto_escala})."
                    )
                else:
                    st.success(
                        f"Mostrando el cierre del **{fecha_elegida}** (últimas **{len(df_grafica)}** lecturas)."
                    )

        else:
            t_inicio_bloque, t_fin_bloque, fmt_hora = mapa_bloques[punto_seleccionado]

            if es_hoy:
                filtrados = [
                    r for r in lista_hoy
                    if t_inicio_bloque <= r["Momento"] < t_fin_bloque
                ]
                df_bloque = pd.DataFrame(filtrados)
            else:
                mascara = (
                    (df_dia_historico["Momento"] >= t_inicio_bloque)
                    & (df_dia_historico["Momento"] < t_fin_bloque)
                )
                df_bloque = df_dia_historico[mascara].copy()

            total_en_bloque = len(df_bloque)

            if total_en_bloque > MAX_PUNTOS_BLOQUE:
                salto = math.ceil(total_en_bloque / MAX_PUNTOS_BLOQUE)
                df_grafica = df_bloque.iloc[::salto].copy()
            else:
                df_grafica = df_bloque

            if not df_bloque.empty:
                df_tabla = df_bloque[["Fecha", "Hora", "Temperatura (°C)"]].iloc[::-1].reset_index(drop=True)
            else:
                df_tabla = pd.DataFrame(columns=["Fecha", "Hora", "Temperatura (°C)"])

            hora_fin_lbl = (
                pd.Timestamp(t_fin_bloque).strftime(fmt_hora).lstrip("0").lower()
            )
            titulo_tabla = (
                f"📋 Lecturas del intervalo ({punto_seleccionado} a {hora_fin_lbl}) "
                f"— {total_en_bloque:,} registro(s)"
            )

            with col_estado_graf:
                st.info(
                    f"⏪ Intervalo seleccionado ({fecha_elegida}): **{punto_seleccionado} a {hora_fin_lbl}** "
                    f"— **{total_en_bloque:,}** lectura(s) en este tramo "
                    f"(haz clic en **{opcion_final}** para volver al seguimiento continuo)."
                )

        # Renderizar gráfica Altair (adaptada tanto a múltiples puntos como a 1 sola lectura)
        if not df_grafica.empty:
            df_plot = df_grafica.copy()
            df_plot["Momento_Plot"] = pd.to_datetime(df_plot["Momento"]).dt.tz_localize(None)
            df_plot["Etiqueta_Unica"] = df_plot.apply(
                lambda r: f"{r['Temperatura (°C)']:.2f} °C ({r['Hora']})", axis=1
            )

            es_lectura_unica = len(df_plot) == 1

            y_min = float(df_plot["Temperatura (°C)"].min())
            y_max = float(df_plot["Temperatura (°C)"].max())
            rango_y = y_max - y_min
            margen_y = max(rango_y * 0.22, 0.5 if es_lectura_unica else 0.05)
            dominio_y = [y_min - margen_y, y_max + margen_y]

            x_min = pd.Timestamp(df_plot["Momento_Plot"].min())
            x_max = pd.Timestamp(df_plot["Momento_Plot"].max())
            rango_x = x_max - x_min
            margen_x = pd.Timedelta(seconds=30) if es_lectura_unica else max(rango_x * 0.05, pd.Timedelta(seconds=2))
            dominio_x = [(x_min - margen_x).isoformat(), (x_max + margen_x).isoformat()]

            eje_x = alt.X(
                "Momento_Plot:T",
                title="Hora",
                scale=alt.Scale(domain=dominio_x),
                axis=alt.Axis(
                    format="%I:%M:%S%p",
                    labelAngle=0,
                    tickCount=5 if es_lectura_unica else 8,
                ),
            )
            eje_y = alt.Y(
                "Temperatura (°C):Q",
                title="Temperatura (°C)",
                scale=alt.Scale(domain=dominio_y, zero=False),
            )

            puntos = (
                alt.Chart(df_plot)
                .mark_circle(
                    size=240 if es_lectura_unica else 125,
                    opacity=1.0 if es_lectura_unica else 0.95,
                    clip=False,
                )
                .encode(
                    x=eje_x,
                    y=eje_y,
                    tooltip=[
                        alt.Tooltip("Fecha:N", title="Fecha"),
                        alt.Tooltip("Hora:N", title="Hora"),
                        alt.Tooltip("Temperatura (°C):Q", title="Temperatura", format=".2f"),
                    ],
                )
            )

            if es_lectura_unica:
                linea_guia = (
                    alt.Chart(df_plot)
                    .mark_rule(strokeDash=[6, 4], strokeWidth=1.8, opacity=0.65)
                    .encode(y=eje_y)
                )
                etiqueta_punto = (
                    alt.Chart(df_plot)
                    .mark_text(dy=-18, fontSize=13, fontWeight="bold")
                    .encode(
                        x=eje_x,
                        y=eje_y,
                        text="Etiqueta_Unica:N",
                    )
                )
                capa_grafica = linea_guia + puntos + etiqueta_punto
            else:
                linea = (
                    alt.Chart(df_plot)
                    .mark_line(strokeWidth=2.2)
                    .encode(x=eje_x, y=eje_y)
                )
                capa_grafica = linea + puntos

            grafica = (
                capa_grafica.properties(
                    height=400,
                    padding={"top": 24, "bottom": 12, "left": 12, "right": 24},
                )
                .interactive()
            )
            st.altair_chart(grafica, use_container_width=True)

            # ==========================================
            # 11. TABLA DESPLAZABLE ACOPLADA AL DÍA, INTERVALO Y PAUSA
            # ==========================================

            st.subheader(titulo_tabla)
            st.dataframe(
                df_tabla,
                use_container_width=True,
                hide_index=True,
                height=380,
                column_config={
                    "Fecha": st.column_config.TextColumn("Fecha", width="medium"),
                    "Hora": st.column_config.TextColumn("Hora", width="medium"),
                    "Temperatura (°C)": st.column_config.NumberColumn(
                        "Temperatura (°C)",
                        format="%.2f °C",
                        width="medium",
                    ),
                },
            )
        else:
            st.warning(f"No se encontraron lecturas dentro del intervalo **{punto_seleccionado}**.")

    else:
        st.info(
            f"Aún no hay lecturas registradas para la fecha **{fecha_elegida}**. "
            f"Esperando datos en el tópico `{TOPIC}`..."
        )

    # ==========================================
    # 12. ERRORES MQTT
    # ==========================================

    if mqtt_error:
        st.error(f"Error de conexión MQTT: {mqtt_error}")


# ==========================================
# 13. EJECUTAR DASHBOARD
# ==========================================

dashboard()
