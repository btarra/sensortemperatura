
        # Alarmas configurables para el flujo seleccionado; registra solo cambios de estado.
        if alerts_enabled and low_limit < high_limit:
            key = f"alarm_state_{device}_{variable}"
            current = float(vals.iloc[-1])
            state = "low" if current < low_limit else "high" if current > high_limit else "normal"
            previous = st.session_state.get(key, "normal")
            if state != previous:
                if state == "low":
                    store.log_alarm(device if device != "Todos" else str(df_day.iloc[-1]["Dispositivo"]), variable if variable != "Todas" else str(df_day.iloc[-1]["Variable"]), current, f"Valor inferior al límite ({low_limit})", "warning")
                elif state == "high":
                    store.log_alarm(device if device != "Todos" else str(df_day.iloc[-1]["Dispositivo"]), variable if variable != "Todas" else str(df_day.iloc[-1]["Variable"]), current, f"Valor superior al límite ({high_limit})", "critical")
                elif previous in ("low", "high"):
                    store.log_alarm(device if device != "Todos" else str(df_day.iloc[-1]["Dispositivo"]), variable if variable != "Todas" else str(df_day.iloc[-1]["Variable"]), current, "La variable volvió al rango configurado", "info")
                st.session_state[key] = state
            if state == "low": st.error(f"🚨 Alerta: {current:.2f} {unit}, por debajo de {low_limit:.2f} {unit}.")
            elif state == "high": st.error(f"🚨 Alerta: {current:.2f} {unit}, por encima de {high_limit:.2f} {unit}.")
            else: st.success("Variable dentro del rango configurado.")

        # Tendencia / suavizado
        st.markdown("#### 📈 Evolución de la variable")
        smooth_window = st.slider("Ventana de media móvil (número de lecturas)", 2, 100, 10)
        plot_df = df_day.copy().sort_values("Momento")
        plot_df["Media móvil"] = plot_df["Valor"].rolling(smooth_window, min_periods=1).mean()
        # Limita puntos dibujados pero conserva extremos.
        if len(plot_df) > LIVE_POINTS:
            idx = sorted(set(list(range(0, len(plot_df), math.ceil(len(plot_df)/LIVE_POINTS))) + [len(plot_df)-1]))
            plot_df = plot_df.iloc[idx]
        base = alt.Chart(plot_df).encode(x=alt.X("Momento:T", title="Hora"))
        raw_line = base.mark_line(opacity=0.35).encode(y=alt.Y("Valor:Q", title=f"Valor ({unit})"), tooltip=["Momento:T", "Dispositivo:N", "Variable:N", alt.Tooltip("Valor:Q", format=".3f")])
        avg_line = base.mark_line(strokeWidth=3).encode(y=alt.Y("Media móvil:Q"), tooltip=["Momento:T", alt.Tooltip("Media móvil:Q", format=".3f")])
        st.altair_chart((raw_line + avg_line).interactive().properties(height=390), use_container_width=True)

        # Indicadores de anomalías estadísticas (IQR); no sustituyen criterio de ingeniería.
        q1, q3 = vals.quantile(.25), vals.quantile(.75)
        iqr = q3 - q1
        anomalies = df_day[(df_day["Valor"] < q1 - 1.5*iqr) | (df_day["Valor"] > q3 + 1.5*iqr)] if iqr > 0 else df_day.iloc[0:0]
        st.caption(f"Detección exploratoria IQR: {len(anomalies)} posible(s) valor(es) atípico(s). Revisar antes de descartar lecturas.")

        with st.expander("📊 Estadística ampliada"):
            stats = pd.DataFrame({"Métrica":["Cantidad","Varianza","Desviación estándar","Percentil 5","Percentil 25","Percentil 50","Percentil 75","Percentil 95","Amplitud"],
                "Resultado":[len(vals), vals.var(), vals.std(), vals.quantile(.05), q1, vals.median(), q3, vals.quantile(.95), vals.max()-vals.min()]})
            st.dataframe(stats, hide_index=True, use_container_width=True)

        # Tabla paginada
        st.markdown("#### 📋 Registros")
        total_rows = len(df_day)
        pages = max(1, math.ceil(total_rows/PAGE_SIZE))
        page = st.number_input("Página", min_value=1, max_value=pages, value=1, step=1)
        start_i = (int(page)-1)*PAGE_SIZE
        st.caption(f"Mostrando {start_i+1:,}–{min(start_i+PAGE_SIZE,total_rows):,} de {total_rows:,} registros")
        st.dataframe(df_day.iloc[::-1].iloc[start_i:start_i+PAGE_SIZE].drop(columns=["id"]), use_container_width=True, hide_index=True)

        # Exportación filtrada y diaria
        csv_bytes = df_day.drop(columns=["id"]).to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ Descargar CSV de la jornada filtrada", csv_bytes, f"lecturas_{selected_day}.csv", "text/csv")
        global_csv = store.export_csv(device=device, variable=variable)
        st.download_button("🌐 Descargar CSV histórico completo (con filtros)", global_csv, "historial_completo.csv", "text/csv")
    else:
        st.info("No hay lecturas para esta jornada y estos filtros. Comprueba el tópico MQTT y el estado de la ESP32.")

    # Comparación de dos jornadas para el mismo dispositivo/variable
    st.divider()
    with st.expander("🔀 Comparar dos jornadas"):
        if len(days) >= 2:
            compare_days = st.multiselect("Selecciona dos jornadas", [x[0] for x in days], default=[x[0] for x in days[:2]], max_selections=2)
            if len(compare_days) == 2:
                frames=[]
                for d in compare_days:
                    start = datetime.fromisoformat(d).replace(tzinfo=TZ)
                    frame = store.query(start, start+timedelta(days=1), device, variable)
                    if not frame.empty:
                        frame = frame.copy()
                        frame["Hora del día"] = frame["Momento"].dt.strftime("%H:%M:%S")
                        frame["Jornada"] = d
                        frames.append(frame)
                if frames:
                    comp = pd.concat(frames, ignore_index=True)
                    comp_chart = alt.Chart(comp).mark_line().encode(x=alt.X("Hora del día:O", sort=None, title="Hora del día"), y=alt.Y("Valor:Q"), color="Jornada:N", tooltip=["Jornada:N","Hora del día:O","Valor:Q"])
                    st.altair_chart(comp_chart.interactive().properties(height=320), use_container_width=True)
                    st.dataframe(comp.groupby("Jornada")["Valor"].agg(["count","min","max","mean","std"]).reset_index(), hide_index=True, use_container_width=True)
                else:
                    st.info("No hay datos para comparar con los filtros seleccionados.")
        else:
            st.caption("Se necesitan al menos dos jornadas registradas.")

    # Registro de eventos y diagnóstico
    st.divider()
    left, right = st.columns(2)
    with left:
        st.subheader("🚨 Historial de alarmas")
        alarm_df = store.alarms()
        if alarm_df.empty: st.caption("Todavía no hay eventos de alarma.")
        else: st.dataframe(alarm_df, use_container_width=True, hide_index=True)
    with right:
        st.subheader("🧰 Diagnóstico")
        diag = pd.DataFrame({"Indicador":["Broker","Filtro suscrito","Último tópico","Mensajes válidos","Mensajes inválidos","Errores SQLite","Reconexiones","Timeout de sensor"],
            "Estado":["Conectado" if broker_ok else "Desconectado", TOPIC_FILTER, last_topic, received, invalid, db_errors, reconnects, f"{TIMEOUT_SECONDS} s"]})
        st.dataframe(diag, use_container_width=True, hide_index=True)
        st.caption("La latencia de extremo a extremo requiere que el emisor incluya timestamp y que ambos relojes estén sincronizados. El QoS 1 reduce pérdidas, pero puede entregar duplicados; para eliminar duplicados de forma robusta añade un message_id generado por la ESP32.")

    # Guía de payload para múltiples sensores
    with st.expander("ℹ️ Formato MQTT compatible"):
        st.markdown("""
        **Formato antiguo (sigue funcionando):** tópico `esp32/temperatura/tarjeta1`, payload `24.6`.

        **Formato recomendado para varios dispositivos/variables:** tópico `esp32/temperatura/tarjeta1`, payload JSON:
        ```json
        {"device_id":"tarjeta1", "variable":"temperatura", "value":24.6, "unit":"°C", "timestamp":"2026-11-18T16:30:00-05:00"}
        ```
        Puedes publicar, por ejemplo, `esp32/humedad/tarjeta1` con `{"value":58.2,"unit":"%"}`. El timestamp es opcional; si se omite, el dashboard usa la hora de recepción en Bogotá.
        """)

    st.caption(f"Base de datos: `{os.path.basename(DB_PATH)}` · Zona horaria: America/Bogota · Actualización de estado: {REFRESH if refresh_enabled else 'manual'}")


render_dashboard()
