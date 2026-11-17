import os
import csv
import math
import time
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

ARCHIVO_RESPALDO = "historial_mqtt.csv"
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
    Genera puntos específicos de división temporal según la escala elegida:
      - 'Horas': divisiones exactas cada 1 hora a lo largo de toda la jornada.
      - 'Minutos': divisiones exactas cada 10 minutos a lo largo de toda la jornada.
      - 'Automática': adapta el paso (segundos, minutos u horas) al volumen de datos.
    El último nodo siempre corresponde a '🔴 En vivo' (o '⏭ Últimas lecturas').
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
# 3. RECOLECTOR MQTT ESCALABLE EN SEGUNDO PLANO
# ==========================================

class RecolectorMQTT:
    def __init__(self):
        self.lock = threading.Lock()
        self.fecha_actual = obtener_ahora().strftime("%Y-%m-%d")

        # Buffer circular en RAM solo para el día en curso (O(1) memoria)
        self.historial_hoy = deque(maxlen=MAX_RAM_DIA)

        # Estadísticas acumuladas en O(1) para el día actual
        self.count_hoy = 0
        self.suma_hoy = 0.0
        self.min_hoy = None
        self.max_hoy = None

        # Metadatos históricos globales (millones de filas en disco)
        self.total_historico = 0
        self.fechas_disponibles = {self.fecha_actual}

        # Estado de conexión Broker MQTT y ESP32
        self.mqtt_connected = False
        self.mqtt_error = None
        self.ultimo_mensaje_ts = None
        self.ultima_hora_esp32 = None

        self._cargar_respaldo_local()
        self._iniciar_cliente()

    def _reiniciar_estadisticas_dia(self, nueva_fecha):
        self.fecha_actual = nueva_fecha
        self.historial_hoy.clear()
        self.count_hoy = 0
        self.suma_hoy = 0.0
        self.min_hoy = None
        self.max_hoy = None
        self.fechas_disponibles.add(nueva_fecha)

    def _registrar_en_estadisticas_hoy(self, temp):
        self.count_hoy += 1
        self.suma_hoy += temp
        if self.min_hoy is None or temp < self.min_hoy:
            self.min_hoy = temp
        if self.max_hoy is None or temp > self.max_hoy:
            self.max_hoy = temp

    def _cargar_respaldo_local(self):
        """
        Lee el CSV por bloques (chunks) para soportar millones de filas sin
        saturar la RAM: indexa las fechas disponibles, cuenta el total global
        y carga en memoria únicamente las lecturas del día actual.
        """
        if not os.path.exists(ARCHIVO_RESPALDO):
            return

        try:
            for chunk in pd.read_csv(ARCHIVO_RESPALDO, chunksize=100_000):
                if chunk.empty or len(chunk.columns) < 3:
                    continue

                chunk = chunk.iloc[:, :3].copy()
                chunk.columns = ["Fecha", "Hora", "Temperatura (°C)"]
                chunk["Fecha"] = chunk["Fecha"].astype(str).str.strip()
                chunk["Temperatura (°C)"] = pd.to_numeric(
                    chunk["Temperatura (°C)"], errors="coerce"
                )
                chunk = chunk.dropna(subset=["Fecha", "Hora", "Temperatura (°C)"])

                self.total_historico += len(chunk)
                self.fechas_disponibles.update(chunk["Fecha"].unique())

                df_hoy = chunk[chunk["Fecha"] == self.fecha_actual]
                if not df_hoy.empty:
                    momentos = pd.to_datetime(
                        df_hoy["Fecha"] + " " + df_hoy["Hora"].astype(str).str.strip(),
                        format="mixed",
                        errors="coerce",
                    )

                    for idx, row in df_hoy.iterrows():
                        ts = momentos.loc[idx]
                        if pd.isna(ts):
                            continue
                        momento_bogota = ts.to_pydatetime().replace(tzinfo=ZONA_HORARIA)
                        temp = float(row["Temperatura (°C)"])

                        self.historial_hoy.append(
                            {
                                "Fecha": row["Fecha"],
                                "Hora": str(row["Hora"]).strip(),
                                "Temperatura (°C)": temp,
                                "Momento": momento_bogota,
                            }
                        )
                        self._registrar_en_estadisticas_hoy(temp)

        except Exception as e:
            self.mqtt_error = f"Error al indexar historial en disco: {e}"

    def _guardar_en_disco(self, registro):
        """Escribe cada lectura directamente en disco (O(1)) sin reescribir el archivo."""
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
        except Exception:
            pass

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
                    self.fechas_disponibles.add(fecha_str)

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

@st.cache_data(ttl=60, show_spinner="Cargando registros del día seleccionado...")
def cargar_datos_dia_historico(fecha_objetivo):
    """Extrae desde disco únicamente las lecturas de un día anterior específico."""
    if not os.path.exists(ARCHIVO_RESPALDO):
        return pd.DataFrame(columns=["Fecha", "Hora", "Temperatura (°C)", "Momento"])

    bloques = []
    for chunk in pd.read_csv(ARCHIVO_RESPALDO, chunksize=100_000):
        if chunk.empty or len(chunk.columns) < 3:
            continue
        chunk = chunk.iloc[:, :3].copy()
        chunk.columns = ["Fecha", "Hora", "Temperatura (°C)"]
        chunk["Fecha"] = chunk["Fecha"].astype(str).str.strip()

        sub = chunk[chunk["Fecha"] == fecha_objetivo].copy()
        if not sub.empty:
            bloques.append(sub)

    if not bloques:
        return pd.DataFrame(columns=["Fecha", "Hora", "Temperatura (°C)", "Momento"])

    df_dia = pd.concat(bloques, ignore_index=True)
    df_dia["Temperatura (°C)"] = pd.to_numeric(
        df_dia["Temperatura (°C)"], errors="coerce"
    )
    df_dia = df_dia.dropna(subset=["Temperatura (°C)"]).reset_index(drop=True)

    momentos = pd.to_datetime(
        df_dia["Fecha"] + " " + df_dia["Hora"].astype(str).str.strip(),
        format="mixed",
        errors="coerce",
    )
    df_dia["Momento"] = momentos.dt.tz_localize(
        ZONA_HORARIA, ambiguous="NaT", nonexistent="NaT"
    )
    df_dia = df_dia.dropna(subset=["Momento"]).reset_index(drop=True)
    return df_dia


@st.cache_data(ttl=60)
def obtener_csv_dia_historico(fecha_objetivo):
    """Genera los bytes CSV de un día anterior cacheado para descarga instantánea."""
    df_dia = cargar_datos_dia_historico(fecha_objetivo)
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
    "continuo por días."
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
        fechas_disp = sorted(recolector.fechas_disponibles | {fecha_hoy}, reverse=True)

        count_hoy = recolector.count_hoy
        suma_hoy = recolector.suma_hoy
        min_hoy = recolector.min_hoy
        max_hoy = recolector.max_hoy

        lista_hoy = list(recolector.historial_hoy)

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

    # Bloque 1: Selección de jornada
    with col_fecha:
        st.subheader("📅 Selección de jornada")

        if st.session_state.fecha_seleccionada not in fechas_disp:
            st.session_state.fecha_seleccionada = fecha_hoy

        fecha_elegida = st.selectbox(
            "Día a visualizar y exportar:",
            options=fechas_disp,
            index=fechas_disp.index(st.session_state.fecha_seleccionada),
            format_func=lambda f: f"{f} (Hoy - Tiempo real)" if f == fecha_hoy else f"{f} (Historial guardado)",
            key="selector_dia_widget",
        )

        if fecha_elegida != st.session_state.fecha_seleccionada:
            st.session_state.fecha_seleccionada = fecha_elegida
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
        df_dia_historico = cargar_datos_dia_historico(fecha_elegida)
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
                    else obtener_csv_dia_historico(fecha_elegida)
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

        st.caption(
            f"Historial general: {total_global:,} lecturas en {len(fechas_disp)} día(s) | "
            f"Jornada ({fecha_elegida}): {total_dia:,} lecturas."
        )

    # ==========================================
    # 9. INDICADORES DE TEMPERATURA Y CONEXIÓN
    # ==========================================

    titulo_kpi = "📊 Indicadores de Hoy (En vivo)" if es_hoy else f"📊 Resumen del {fecha_elegida}"
    st.subheader(titulo_kpi)

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    if temp_actual is not None:
        etiqueta_ultima = "Temperatura Actual" if es_hoy else "Última del Día"
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
        delta=f"{total_dia:,} lecturas hoy" if es_hoy else f"{total_dia:,} lecturas",
        delta_color="off",
    )

    # ==========================================
    # 10. GRÁFICA CON PUNTOS DE DIVISIÓN Y PAUSA SINCRONIZADA
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
                    f"— **{total_en_bloque:,}** lecturas en este tramo "
                    f"(haz clic en **{opcion_final}** para volver al seguimiento continuo)."
                )

        # Renderizar gráfica Altair con márgenes amplios para que ningún punto quede cortado
        if not df_grafica.empty:
            y_min = float(df_grafica["Temperatura (°C)"].min())
            y_max = float(df_grafica["Temperatura (°C)"].max())
            rango_y = y_max - y_min
            margen_y = max(rango_y * 0.22, 0.05)
            dominio_y = [y_min - margen_y, y_max + margen_y]

            x_min = pd.Timestamp(df_grafica["Momento"].min())
            x_max = pd.Timestamp(df_grafica["Momento"].max())
            rango_x = x_max - x_min
            margen_x = max(rango_x * 0.05, pd.Timedelta(seconds=2))
            dominio_x = [x_min - margen_x, x_max + margen_x]

            puntos = (
                alt.Chart(df_grafica)
                .mark_circle(size=125, opacity=0.95, clip=False)
                .encode(
                    x=alt.X(
                        "Momento:T",
                        title="Hora",
                        scale=alt.Scale(domain=dominio_x),
                        axis=alt.Axis(format="%-I:%M:%S%p", labelAngle=0, tickCount=8),
                    ),
                    y=alt.Y(
                        "Temperatura (°C):Q",
                        title="Temperatura (°C)",
                        scale=alt.Scale(domain=dominio_y, zero=False),
                    ),
                    tooltip=[
                        alt.Tooltip("Fecha:N", title="Fecha"),
                        alt.Tooltip("Hora:N", title="Hora"),
                        alt.Tooltip("Temperatura (°C):Q", title="Temperatura", format=".2f"),
                    ],
                )
            )

            linea = (
                alt.Chart(df_grafica)
                .mark_line(strokeWidth=2.2)
                .encode(
                    x=alt.X(
                        "Momento:T",
                        title="Hora",
                        scale=alt.Scale(domain=dominio_x),
                        axis=alt.Axis(format="%-I:%M:%S%p", labelAngle=0, tickCount=8),
                    ),
                    y=alt.Y(
                        "Temperatura (°C):Q",
                        title="Temperatura (°C)",
                        scale=alt.Scale(domain=dominio_y, zero=False),
                    ),
                )
            )

            grafica = (
                (linea + puntos)
                .properties(
                    height=400,
                    padding={"top": 20, "bottom": 12, "left": 12, "right": 24},
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
