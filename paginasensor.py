import os
import csv
import math
import time
import threading
from collections import deque
from datetime import datetime
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

# Ventana de puntos visibles simultáneamente en la gráfica
MAX_MOSTRAR = 30
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


# ==========================================
# 2. UTILIDADES DE FECHA Y HORA
# ==========================================

def obtener_ahora():
    return datetime.now(ZONA_HORARIA)


def formato_hora(fecha_hora):
    return fecha_hora.strftime("%I:%M:%S%p").lstrip("0").lower()


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

                # Filtrar solo los registros correspondientes al día de hoy
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
                    # Si cruzó la medianoche, archiva el día anterior y limpia RAM
                    if fecha_str != self.fecha_actual:
                        self._reiniciar_estadisticas_dia(fecha_str)

                    self.historial_hoy.append(registro)
                    self._registrar_en_estadisticas_hoy(temperatura)
                    self.total_historico += 1
                    self.fechas_disponibles.add(fecha_str)

                    # Actualizar latido (heartbeat) de la ESP32
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
# 4. CARGA CACHEADA DE DÍAS ANTERIORES
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


# ==========================================
# 5. ESTADO DE NAVEGACIÓN DE LA INTERFAZ
# ==========================================

if "grafica_pausada" not in st.session_state:
    st.session_state.grafica_pausada = False

if "offset_grafica" not in st.session_state:
    # 0 = extremo más reciente (en vivo). > 0 = número de lecturas hacia atrás.
    st.session_state.offset_grafica = 0

if "fecha_seleccionada" not in st.session_state:
    st.session_state.fecha_seleccionada = obtener_ahora().strftime("%Y-%m-%d")


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

        # Estadísticas O(1) de hoy
        count_hoy = recolector.count_hoy
        suma_hoy = recolector.suma_hoy
        min_hoy = recolector.min_hoy
        max_hoy = recolector.max_hoy

        # Copia rápida de la lista del día actual
        lista_hoy = list(recolector.historial_hoy)

    # Calcular estado real de la ESP32 (latido menor a TIMEOUT_ESP32_SEGUNDOS)
    if ultimo_ts is not None:
        segundos_sin_datos = time.monotonic() - ultimo_ts
        esp32_conectada = segundos_sin_datos <= TIMEOUT_ESP32_SEGUNDOS
    else:
        segundos_sin_datos = None
        esp32_conectada = False

    # ==========================================
    # 8. SELECTOR DE DÍA Y EXPORTACIÓN DIRECTA
    # ==========================================

    col_fecha, col_export = st.columns([2, 2])

    with col_fecha:
        st.subheader("📅 Selección de jornada")

        if st.session_state.fecha_seleccionada not in fechas_disp:
            st.session_state.fecha_seleccionada = fecha_hoy

        fecha_elegida = st.selectbox(
            "Día a visualizar:",
            options=fechas_disp,
            index=fechas_disp.index(st.session_state.fecha_seleccionada),
            format_func=lambda f: f"{f} (Hoy - Tiempo real)" if f == fecha_hoy else f"{f} (Historial guardado)",
            key="selector_dia_widget",
        )

        # Si el usuario cambió de día, reiniciar el desplazamiento de la gráfica
        if fecha_elegida != st.session_state.fecha_seleccionada:
            st.session_state.fecha_seleccionada = fecha_elegida
            st.session_state.offset_grafica = 0
            st.session_state.grafica_pausada = False
            st.rerun()

    es_hoy = (fecha_elegida == fecha_hoy)

    with col_export:
        st.subheader("📥 Exportación CSV")
        if os.path.exists(ARCHIVO_RESPALDO) and total_global > 0:
            # Lectura directa desde disco sin reconstruir DataFrames cada segundo
            with open(ARCHIVO_RESPALDO, "rb") as archivo_csv:
                st.download_button(
                    label=f"📥 Descargar historial completo ({total_global:,} lecturas)",
                    data=archivo_csv,
                    file_name="historial_completo_temperaturas.csv",
                    mime="text/csv",
                    key="descargar_csv_global",
                )
            st.caption(
                f"Total almacenado en disco: {total_global:,} registros | "
                f"Días registrados: {len(fechas_disp)}"
            )
        else:
            st.button(
                "📥 Descargar historial completo CSV",
                disabled=True,
                key="descargar_csv_vacio",
            )
            st.caption("Esperando lecturas para generar el respaldo CSV.")

    # ==========================================
    # 9. PREPARACIÓN DE DATOS DEL DÍA ELEGIDO
    # ==========================================

    if es_hoy:
        total_dia = len(lista_hoy)
        if total_dia > 0:
            temp_actual = lista_hoy[-1]["Temperatura (°C)"]
            temp_min = min_hoy
            temp_max = max_hoy
            temp_prom = suma_hoy / count_hoy
        else:
            temp_actual = temp_min = temp_max = temp_prom = None
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

    # ==========================================
    # 10. INDICADORES DE TEMPERATURA Y CONEXIÓN
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

    # Estado específico de la tarjeta ESP32
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

    # Estado de conexión con el Broker MQTT
    col6.metric(
        "Broker MQTT",
        "Conectado 🟢" if mqtt_conectado else "Desconectado 🔴",
        delta=f"{total_dia:,} lecturas hoy" if es_hoy else f"{total_dia:,} lecturas",
        delta_color="off",
    )

    # ==========================================
    # 11. GRÁFICA INTERACTIVA CON RETROCESO
    # ==========================================

    st.subheader("📈 Comportamiento térmico")

    if total_dia > 0:
        max_offset = max(0, total_dia - MAX_MOSTRAR)

        # Si el usuario retrocedió manualmente, mantener su posición relativa aunque lleguen nuevos datos
        if st.session_state.offset_grafica > max_offset:
            st.session_state.offset_grafica = max_offset

        # Barra de controles de navegación temporal
        c_ini, c_ret30, c_ret10, c_pausa, c_ava10, c_ava30, c_vivo = st.columns(7)

        with c_ini:
            if st.button("⏮ Inicio", use_container_width=True, disabled=(st.session_state.offset_grafica >= max_offset)):
                st.session_state.offset_grafica = max_offset
                st.session_state.grafica_pausada = True
                st.rerun()

        with c_ret30:
            if st.button(f"⏪ -{MAX_MOSTRAR}", use_container_width=True, disabled=(st.session_state.offset_grafica >= max_offset)):
                st.session_state.offset_grafica = min(max_offset, st.session_state.offset_grafica + MAX_MOSTRAR)
                st.session_state.grafica_pausada = True
                st.rerun()

        with c_ret10:
            if st.button("◀ -10", use_container_width=True, disabled=(st.session_state.offset_grafica >= max_offset)):
                st.session_state.offset_grafica = min(max_offset, st.session_state.offset_grafica + 10)
                st.session_state.grafica_pausada = True
                st.rerun()

        with c_pausa:
            if st.session_state.grafica_pausada:
                if st.button("▶ Reanudar", use_container_width=True):
                    st.session_state.grafica_pausada = False
                    st.rerun()
            else:
                if st.button("⏸ Pausar", use_container_width=True):
                    st.session_state.grafica_pausada = True
                    st.rerun()

        with c_ava10:
            if st.button("▶ +10", use_container_width=True, disabled=(st.session_state.offset_grafica == 0)):
                st.session_state.offset_grafica = max(0, st.session_state.offset_grafica - 10)
                st.rerun()

        with c_ava30:
            if st.button(f"⏩ +{MAX_MOSTRAR}", use_container_width=True, disabled=(st.session_state.offset_grafica == 0)):
                st.session_state.offset_grafica = max(0, st.session_state.offset_grafica - MAX_MOSTRAR)
                st.rerun()

        with c_vivo:
            en_extremo_reciente = (st.session_state.offset_grafica == 0 and not st.session_state.grafica_pausada)
            etiqueta_vivo = "🔴 En vivo" if es_hoy else "⏭ Final del día"
            if st.button(etiqueta_vivo, use_container_width=True, disabled=en_extremo_reciente):
                st.session_state.offset_grafica = 0
                st.session_state.grafica_pausada = False
                st.rerun()

        # Si está pausado en vivo (offset == 0) y entran datos nuevos, incrementar offset
        # para congelar exactamente la ventana que el usuario está observando
        if es_hoy and st.session_state.grafica_pausada and st.session_state.offset_grafica == 0:
            pass

        # Calcular ventana exacta [inicio_idx : fin_idx] en O(1)
        fin_idx = total_dia - st.session_state.offset_grafica
        inicio_idx = max(0, fin_idx - MAX_MOSTRAR)

        if es_hoy:
            # Solo se convierte a DataFrame la ventana visible de 30 puntos (O(1))
            df_grafica = pd.DataFrame(lista_hoy[inicio_idx:fin_idx])
        else:
            df_grafica = df_dia_historico.iloc[inicio_idx:fin_idx].copy()

        # Mensaje de estado de la ventana visualizada
        if st.session_state.offset_grafica > 0:
            st.info(
                f"⏪ Modo historial ({fecha_elegida}): Mostrando lecturas "
                f"**{inicio_idx + 1:,}** a **{fin_idx:,}** de **{total_dia:,}** "
                f"(retrocedido {st.session_state.offset_grafica:,} lecturas)."
            )
        elif st.session_state.grafica_pausada:
            st.warning(
                f"⏸ Gráfica pausada en lecturas **{inicio_idx + 1:,}** a **{fin_idx:,}** "
                f"de **{total_dia:,}**. Las nuevas lecturas siguen guardándose."
            )
        else:
            modo_texto = "actualizándose en tiempo real" if es_hoy else "mostrando cierre del día"
            st.success(
                f"Mostrando últimas lecturas (**{inicio_idx + 1:,}** a **{fin_idx:,}** "
                f"de **{total_dia:,}**) — {modo_texto}."
            )

        # Renderizar gráfica Altair
        puntos = (
            alt.Chart(df_grafica)
            .mark_circle(size=140, opacity=0.9)
            .encode(
                x=alt.X(
                    "Momento:T",
                    title="Hora",
                    axis=alt.Axis(format="%-I:%M:%S%p", labelAngle=0),
                ),
                y=alt.Y(
                    "Temperatura (°C):Q",
                    title="Temperatura (°C)",
                    scale=alt.Scale(zero=False),
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
            .mark_line(strokeWidth=2)
            .encode(
                x=alt.X(
                    "Momento:T",
                    title="Hora",
                    axis=alt.Axis(format="%-I:%M:%S%p", labelAngle=0),
                ),
                y=alt.Y(
                    "Temperatura (°C):Q",
                    title="Temperatura (°C)",
                    scale=alt.Scale(zero=False),
                ),
            )
        )

        grafica = (linea + puntos).properties(height=360).interactive()
        st.altair_chart(grafica, use_container_width=True)

        # ==========================================
        # 12. TABLA DE LECTURAS DE LA VENTANA ACTUAL
        # ==========================================

        st.subheader("📋 Lecturas de la ventana seleccionada")
        df_tabla = df_grafica[["Fecha", "Hora", "Temperatura (°C)"]].tail(10).iloc[::-1]
        st.dataframe(df_tabla, use_container_width=True, hide_index=True)

    else:
        st.info(
            f"Aún no hay lecturas registradas para la fecha **{fecha_elegida}**. "
            f"Esperando datos en el tópico `{TOPIC}`..."
        )

    # ==========================================
    # 13. ERRORES MQTT
    # ==========================================

    if mqtt_error:
        st.error(f"Error de conexión MQTT: {mqtt_error}")


# ==========================================
# 14. EJECUTAR DASHBOARD
# ==========================================

dashboard()
