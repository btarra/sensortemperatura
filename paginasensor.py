                        )
                    )
                    capa_grafica = capa_grafica + linea_media

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

            # Estadística descriptiva adicional para complementar los indicadores existentes.
            with st.expander("📐 Análisis estadístico de la jornada", expanded=False):
                df_analisis = (
                    pd.DataFrame(lista_hoy) if es_hoy
                    else df_dia_historico.copy()
                )
                if not df_analisis.empty:
                    serie = pd.to_numeric(df_analisis["Temperatura (°C)"], errors="coerce").dropna()
                    if not serie.empty:
                        a1, a2, a3, a4, a5 = st.columns(5)
                        a1.metric("Mediana", f"{serie.median():.2f} °C")
                        a2.metric("Desviación estándar", f"{serie.std(ddof=0):.2f} °C")
                        a3.metric("Percentil 5", f"{serie.quantile(0.05):.2f} °C")
                        a4.metric("Percentil 95", f"{serie.quantile(0.95):.2f} °C")
                        a5.metric("Amplitud", f"{serie.max() - serie.min():.2f} °C")
                        if es_hoy and total_dia > len(df_analisis):
                            st.caption(
                                f"Estadísticas calculadas sobre las {len(df_analisis):,} lecturas más recientes disponibles en RAM; "
                                f"la jornada completa contiene {total_dia:,}."
                            )

            # Comparación opcional entre dos jornadas, sin cambiar la selección principal.
            fechas_comparables = [f for f in fechas_disp if f != fecha_elegida and conteos_fechas.get(f, 0) > 0]
            with st.expander("📊 Comparar con otra jornada", expanded=False):
                if fechas_comparables:
                    fecha_comparacion = st.selectbox(
                        "Selecciona la jornada de comparación:",
                        options=fechas_comparables,
                        key=f"comparar_dia_{fecha_elegida}",
                    )
                    df_comparacion = cargar_datos_dia_historico(
                        fecha_comparacion, conteos_fechas.get(fecha_comparacion, 0)
                    )
                    df_base_comparacion = (
                        pd.DataFrame(lista_hoy) if es_hoy else df_dia_historico
                    )
                    if not df_comparacion.empty and not df_base_comparacion.empty:
                        promedio_base = df_base_comparacion["Temperatura (°C)"].mean()
                        promedio_comp = df_comparacion["Temperatura (°C)"].mean()
                        diferencia = promedio_base - promedio_comp
                        b1, b2, b3 = st.columns(3)
                        b1.metric(f"Promedio {fecha_elegida}", f"{promedio_base:.2f} °C")
                        b2.metric(f"Promedio {fecha_comparacion}", f"{promedio_comp:.2f} °C")
                        b3.metric("Diferencia de promedios", f"{diferencia:+.2f} °C")
                        st.caption("La diferencia corresponde al promedio de la jornada seleccionada menos el promedio de la jornada comparada.")
                else:
                    st.info("Se necesita al menos otra jornada con registros para realizar una comparación.")
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

    with st.expander("🛠️ Diagnóstico de recepción MQTT", expanded=False):
        d1, d2, d3 = st.columns(3)
        d1.metric("Mensajes válidos recibidos", f"{mensajes_recibidos:,}")
        d2.metric("Mensajes descartados", f"{mensajes_invalidos:,}")
        d3.metric("Tópico configurado", TOPIC)
        st.write(f"**Broker:** `{BROKER_IP}:{BROKER_PORT}`")
        st.write(f"**Último tópico recibido:** `{ultimo_topic or 'Aún no hay mensajes'}`")
        st.write(f"**Último payload:** `{ultimo_payload or 'Aún no hay mensajes'}`")
        if ultimo_error_payload:
            st.warning(f"Último mensaje descartado: {ultimo_error_payload}")
        st.caption(
            "El dashboard acepta el payload numérico original (por ejemplo, 24.5) y también JSON "
            "con una clave temperatura, temperature, temp o value."
        )

    if mqtt_error:
        st.error(f"Error de conexión MQTT: {mqtt_error}")


# ==========================================
# 13. EJECUTAR DASHBOARD
# ==========================================

dashboard()
