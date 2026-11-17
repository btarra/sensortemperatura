import os
import csv
import math
import threading
import altair as alt
import pandas as pd
import paho.mqtt.client as mqtt
import streamlit as st

from datetime import datetime
from zoneinfo import ZoneInfo

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================

BROKER_IP = "broker.hivemq.com"
BROKER_PORT = 1883
TOPIC = "esp32/temperatura/tarjeta1"

ARCHIVO_RESPALDO = "historial_mqtt.csv"
MAX_MOSTRAR = 30
INTERVALO_ACTUALIZACION = "1s"


st.set_page_config(
    page_title="Visualización de Variables",
    page_icon="🌡️",
    layout="wide",
)


# ==========================================
# 2. FORMATO DE HORA
# ==========================================

def formato_hora(fecha_hora):
    return fecha_hora.strftime("%I:%M:%S%p").lstrip("0").lower()


# ==========================================
# 3. RECOLECTOR MQTT GLOBAL EN SEGUNDO PLANO
# ==========================================

class RecolectorMQTT:
    def __init__(self):
        self.lock = threading.Lock()
        self.historial = []
        self.mqtt_connected = False
        self.mqtt_error = None
        self._cargar_respaldo_local()
        self._iniciar_cliente()

    def _cargar_respaldo_local(self):
        """Recupera lecturas previas guardadas en disco si existen."""
        if not os.path.exists(ARCHIVO_RESPALDO):
            return

        try:
            df_previo = pd.read_csv(ARCHIVO_RESPALDO)
            if df_previo.empty:
                return

            momentos = pd.to_datetime(
                df_previo["Fecha"].astype(str) + " " + df_previo["Hora"].astype(str),
                format="mixed",
                errors="coerce",
            )

            for idx, row in df_previo.iterrows():
                ts = momentos.loc[idx]
                if pd.isna(ts):
                    continue
                momento_bogota = ts.to_pydatetime().replace(
                    tzinfo=ZoneInfo("America/Bogota")
                )
                self.historial.append(
                    {
                        "Fecha": str(row["Fecha"]),
                        "Hora": str(row["Hora"]),
                        "Temperatura (°C)": float(row["Temperatura (°C)"]),
                        "Momento": momento_bogota,
                    }
                )
        except Exception:
            pass

    def _guardar_en_disco(self, registro):
        """Guarda cada lectura nueva en el CSV local inmediatamente al recibirla."""
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

                ahora = datetime.now(ZoneInfo("America/Bogota"))
                registro = {
                    "Fecha": ahora.strftime("%Y-%m-%d"),
                    "Hora": formato_hora(ahora),
                    "Temperatura (°C)": temperatura,
                    "Momento": ahora,
                }

                with self.lock:
                    self.historial.append(registro)
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
    """Crea una única instancia compartida por todo el servidor Streamlit."""
    return RecolectorMQTT()


recolector = obtener_recolector()


# ==========================================
# 4. ESTADO DE LA INTERFAZ (POR PESTAÑA)
# ==========================================

if "grafica_pausada" not in st.session_state:
    st.session_state.grafica_pausada = False

if "historial_grafica" not in st.session_state:
    st.session_state.historial_grafica = pd.DataFrame()


# ==========================================
# 5. INTERFAZ PRINCIPAL
# ==========================================

st.title("🌡️ Dashboard: Monitoreo Remoto Wi-Fi")

st.markdown(
    "Visualización en tiempo real de las lecturas del sensor "
    "**DS18B20**, transmitidas mediante una **ESP32** "
    "utilizando el protocolo MQTT con captura continua en segundo plano."
)

st.divider()


# ==========================================
# 6. ACTUALIZACIÓN DEL DASHBOARD
# ==========================================

@st.fragment(run_every=INTERVALO_ACTUALIZACION)
def dashboard():

    # Leer copia segura del historial acumulado en segundo plano
    with recolector.lock:
        datos_actuales = list(recolector.historial)
        mqtt_conectado = recolector.mqtt_connected
        mqtt_error = recolector.mqtt_error

    df = pd.DataFrame(
        datos_actuales,
        columns=["Fecha", "Hora", "Temperatura (°C)", "Momento"],
    )

    df_visual = df.tail(MAX_MOSTRAR)

    # ==========================================
    # 7. EXPORTACIÓN CSV
    # ==========================================

    st.subheader("📥 Exportación de datos")

    if not df.empty:
        df_csv = df[["Fecha", "Hora", "Temperatura (°C)"]]
        csv_bytes = df_csv.to_csv(index=False).encode("utf-8-sig")

        st.download_button(
            label="📥 Descargar historial completo CSV",
            data=csv_bytes,
            file_name="historial_completo_temperaturas.csv",
            mime="text/csv",
            key="descargar_csv",
        )

        st.caption(
            f"Registros almacenados: {len(df)} | "
            f"Registros exportados: {len(df)}"
        )
    else:
        st.button(
            "📥 Descargar historial completo CSV",
            disabled=True,
            key="descargar_csv_vacio",
        )
        st.caption("Esperando datos para generar el archivo CSV.")

    # ==========================================
    # 8. INDICADORES DE TEMPERATURA
    # ==========================================

    st.subheader("📊 Indicadores")

    if not df.empty:
        temperatura_actual = df["Temperatura (°C)"].iloc[-1]
        temperatura_minima = df["Temperatura (°C)"].min()
        temperatura_maxima = df["Temperatura (°C)"].max()
        temperatura_promedio = df["Temperatura (°C)"].mean()

        col1, col2, col3, col4, col5 = st.columns(5)

        col1.metric("Temperatura Actual", f"{temperatura_actual:.2f} °C")
        col2.metric("Temperatura Mínima", f"{temperatura_minima:.2f} °C")
        col3.metric("Temperatura Máxima", f"{temperatura_maxima:.2f} °C")
        col4.metric("Temperatura Promedio", f"{temperatura_promedio:.2f} °C")
        col5.metric("Estado de Red", "Conectado 🟢" if mqtt_conectado else "Desconectado 🔴")
    else:
        col1, col2, col3, col4, col5 = st.columns(5)

        col1.metric("Temperatura Actual", "-- °C")
        col2.metric("Temperatura Mínima", "-- °C")
        col3.metric("Temperatura Máxima", "-- °C")
        col4.metric("Temperatura Promedio", "-- °C")
        col5.metric("Estado de Red", "Conectado 🟢" if mqtt_conectado else "Desconectado 🔴")

    # ==========================================
    # 9. GRÁFICA INTERACTIVA
    # ==========================================

    st.subheader("📈 Temperatura en tiempo real")

    col_boton, col_estado = st.columns([1, 3])

    with col_boton:
        if st.session_state.grafica_pausada:
            if st.button("▶ Reanudar gráfica", key="reanudar_grafica"):
                st.session_state.grafica_pausada = False
                st.rerun()
        else:
            if st.button("⏸ Pausar gráfica", key="pausar_grafica"):
                st.session_state.grafica_pausada = True
                st.rerun()

    with col_estado:
        if st.session_state.grafica_pausada:
            st.info("Gráfica pausada. Las lecturas siguen almacenándose en segundo plano.")
        else:
            st.success("Gráfica actualizándose.")

    if not df.empty:
        if not st.session_state.grafica_pausada:
            st.session_state.historial_grafica = df.tail(MAX_MOSTRAR).copy()

        df_grafica = st.session_state.historial_grafica.copy()

        if not df_grafica.empty:
            fecha_inicio = df_grafica["Fecha"].iloc[0]
            fecha_fin = df_grafica["Fecha"].iloc[-1]

            if fecha_inicio == fecha_fin:
                st.caption(f"Fecha de las lecturas: {fecha_inicio}")
            else:
                st.caption(f"Periodo de las lecturas: {fecha_inicio} a {fecha_fin}")

            puntos = (
                alt.Chart(df_grafica)
                .mark_circle(size=150, opacity=0.9)
                .encode(
                    x=alt.X(
                        "Momento:T",
                        title="Hora",
                        axis=alt.Axis(format="%-I:%M:%S%p", labelAngle=0),
                    ),
                    y=alt.Y("Temperatura (°C):Q", title="Temperatura (°C)"),
                    tooltip=[
                        alt.Tooltip("Fecha:N", title="Fecha"),
                        alt.Tooltip("Hora:N", title="Hora"),
                        alt.Tooltip("Temperatura (°C):Q", title="Temperatura", format=".2f"),
                    ],
                )
            )

            linea = (
                alt.Chart(df_grafica)
                .mark_line(strokeWidth=2)
                .encode(
                    x=alt.X(
                        "Momento:T",
                        title="Hora",
                        axis=alt.Axis(format="%-I:%M:%S%p", labelAngle=0),
                    ),
                    y=alt.Y("Temperatura (°C):Q", title="Temperatura (°C)"),
                )
            )

            grafica = (linea + puntos).properties(height=350).interactive()
            st.altair_chart(grafica, use_container_width=True)
    else:
        st.info("Esperando datos para generar la gráfica.")

    # ==========================================
    # 10. TABLA DE ÚLTIMAS LECTURAS
    # ==========================================

    st.subheader("📋 Últimas lecturas")

    if not df.empty:
        df_tabla = df_visual[["Fecha", "Hora", "Temperatura (°C)"]].tail(5)
        st.dataframe(df_tabla, use_container_width=True, hide_index=True)
    else:
        st.info(f"Esperando datos del sensor. Tópico MQTT: {TOPIC}")

    # ==========================================
    # 11. ERRORES MQTT
    # ==========================================

    if mqtt_error:
        st.error(f"Error de conexión MQTT: {mqtt_error}")


# ==========================================
# 12. EJECUTAR DASHBOARD
# ==========================================

dashboard()
