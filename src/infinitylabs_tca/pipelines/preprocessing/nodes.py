import pandas as pd

def process_dataframes(
    agencias: pd.DataFrame,
    canales: pd.DataFrame,
    empresas: pd.DataFrame,
    estatus_reservaciones: pd.DataFrame,
    paises: pd.DataFrame,
    paquetes: pd.DataFrame,
    reservaciones: pd.DataFrame,
    segmentos_comp: pd.DataFrame,
    tipos_habitaciones: pd.DataFrame
) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    
    reservaciones['client_key'] = reservaciones.groupby(['h_nom', 'h_edo']).ngroup()


    # Convert columns to datetime
    columns_to_convert = ['Fecha_hoy', 'h_res_fec_okt', 'h_fec_lld_okt', 'h_fec_reg_okt', 'h_fec_sda_okt', 'h_ult_cam_fec_okt']
    for column in columns_to_convert:
        reservaciones[column] = pd.to_datetime(reservaciones[column])

    # Combine all conditions in a single step using logical AND (&)
    reservaciones = reservaciones[
        (reservaciones['Fecha_hoy'].dt.year != 2000) &
        (reservaciones['h_res_fec_okt'].dt.year != 2000) &
        (reservaciones['h_fec_lld_okt'].dt.year != 2000) &
        (reservaciones['h_fec_reg_okt'].dt.year != 2000) &
        (reservaciones['h_fec_sda_okt'].dt.year != 2000) &
        (reservaciones['h_ult_cam_fec_okt'].dt.year != 2000)
    ]

    reservaciones_model = reservaciones.copy()

    reservaciones_model = reservaciones_model[reservaciones_model['h_num_per'] != 0]

    max_date = reservaciones['h_res_fec_okt'].max()

    features_model = reservaciones_model.groupby('client_key').agg(
        num_visits=('h_fec_lld_okt', 'nunique'),
        avg_days_between_visits=('h_res_fec_okt', lambda x: (x.diff().mean().days if x.count() > 1 else 0)),
        last_visit=('h_fec_lld_okt', 'max'),
        last_reservation=('h_fec_lld_okt', 'max'),
        dias_estancia=('h_num_noc','mean'),
        total_rooms_reserved=('h_tot_hab', 'sum'),
        avg_rooms_reserved=('h_tot_hab', 'mean'),
        total_people_stayed=('h_num_per', 'sum'),
        avg_people_stayed=('h_num_per', 'mean'),
        total_adults=('h_num_adu', 'sum'),
        avg_adults=('h_num_adu', 'mean'),
        total_children=('h_num_men', 'sum'),
        avg_children=('h_num_men', 'mean'),
        origin_country=('ID_Pais_Origen', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        segmento_comp=('ID_Segmento_Comp', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        most_common_canal=('ID_canal', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        most_common_agency=('ID_Agencia', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        most_common_package=('ID_Paquete', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        most_common_room_type=('ID_Tipo_Habitacion', lambda x: x.mode().iloc[0] if not x.mode().empty else None)
    ).reset_index()


    features_model['time_since_last_res'] = max_date-features_model['last_reservation']
    features_model['time_since_last_res'] = features_model['time_since_last_res'].dt.days
    features_model['churn'] = features_model['time_since_last_res'] > 365

    paquetes.rename(columns={'Paquete_nombre': 'paquete'}, inplace=True)
    reservaciones = reservaciones.merge(paquetes[['ID_paquete', 'paquete']], 
                                        left_on='ID_Paquete', right_on='ID_paquete', how='left')
    reservaciones.drop(columns=['ID_paquete', 'ID_Paquete'], inplace=True)


    # Map canal to reservaciones
    canales.rename(columns={'Canal_nombre': 'canal'}, inplace=True)
    reservaciones = reservaciones.merge(canales[['ID_canal', 'canal']], 
                                        left_on='ID_canal', right_on='ID_canal', how='left')
    reservaciones.drop(columns=['ID_canal'], inplace=True)


    # Map agencia to reservaciones
    agencias.rename(columns={'Agencia_nombre': 'agencia'}, inplace=True)
    reservaciones = reservaciones.merge(agencias[['ID_Agencia', 'agencia']],
                                        left_on='ID_Agencia', right_on='ID_Agencia', how='left')
    reservaciones.drop(columns=['ID_Agencia'], inplace=True)


    # Map segmentos to reservaciones
    segmentos_comp.rename(columns={'Segmento_Comp_Nombre': 'segmento'}, inplace=True)
    reservaciones = reservaciones.merge(segmentos_comp[['ID_Segmento_Comp', 'segmento']],
                                        left_on='ID_Segmento_Comp', right_on='ID_Segmento_Comp', how='left')
    reservaciones.drop(columns=['ID_Segmento_Comp'], inplace=True)


    # Map tipos_habitaciones to reservaciones
    tipos_habitaciones.rename(columns={'Tipo_Habitacion_nombre': 'tipo_habitacion'}, inplace=True)
    reservaciones = reservaciones.merge(tipos_habitaciones[['ID_Tipo_Habitacion', 'tipo_habitacion']],
                                        left_on='ID_Tipo_Habitacion', right_on='ID_Tipo_Habitacion', how='left')
    reservaciones.drop(columns=['ID_Tipo_Habitacion'], inplace=True)


    # Map estatus_reservaciones to reservaciones
    estatus_reservaciones.rename(columns={'estatus_reservaciones': 'estatus_reservacion'}, inplace=True)
    reservaciones = reservaciones.merge(estatus_reservaciones[['ID_estatus_reservaciones', 'estatus_reservacion']],                  
                                        left_on='ID_estatus_reservaciones', right_on='ID_estatus_reservaciones', how='left')
    reservaciones.drop(columns=['ID_estatus_reservaciones'], inplace=True)


    # Map empresas to reservaciones
    paises.rename(columns={'Pais_Nombre': 'pais'}, inplace=True)
    paises.rename(columns={'ID_Pais': 'ID_Pais_Origen'}, inplace=True)
    reservaciones = reservaciones.merge(paises[['ID_Pais_Origen', 'pais']],
                                        left_on='ID_Pais_Origen', right_on='ID_Pais_Origen', how='left')


    empresas.rename(columns={'Empresa_nombre': 'empresa'}, inplace=True)
    reservaciones = reservaciones.merge(empresas[['ID_empresa', 'empresa']],
                                        left_on='ID_empresa', right_on='ID_empresa', how='left')
    reservaciones.drop(columns=['ID_empresa'], inplace=True)

    reservaciones.drop(columns=['ID_Pais_Origen'], inplace=True)

    # Select useful and final columns
    final_cols = ['client_key', 'ID_Reserva', 'Fecha_hoy', 'h_res_fec_okt',
        'h_num_per', 'h_num_adu', 'h_num_men',
        'h_num_noc', 'h_tot_hab',
        'h_fec_lld_okt',
        'h_fec_reg_okt', 'h_fec_sda_okt',
        'Cliente_Disp', 'Reservacion', 
        'h_can_res', 'h_cod_reserva', 'h_edo', 'h_codigop', 'h_correo_e',
        'h_nom', 'h_tfa_total', 'moneda_cve', 'h_ult_cam_fec_okt', 'paquete', 'canal', 'agencia',
        'segmento', 'tipo_habitacion', 'estatus_reservacion', 'pais', 'empresa']
    reservaciones = reservaciones[final_cols]

    # Split tipo_habitacion to get actual value
    reservaciones['Tipo_Habitacion_nombre_split'] = reservaciones['tipo_habitacion'].str.extract(r'(.+?)(?:\s*SN12)')[0]
    reservaciones.drop(columns=['tipo_habitacion'], inplace=True)

    # Rename columns 
    rename_cols = {'ID_Reserva': 'id_reserva', 'Fecha_hoy': 'fecha_hoy', 'h_res_fec_okt': 'fecha_reservacion',
                    'h_num_per': 'num_personas', 'h_num_adu': 'num_adultos',
                    'h_num_men': 'num_menores', 'h_num_noc': 'num_noches',
                    'h_tot_hab': 'num_habitaciones', 'ID_Programa': 'id_programa',
                    'h_fec_lld_okt': 'fecha_llegada', 'h_fec_reg_okt': 'fecha_registro',
                    'h_fec_sda_okt': 'fecha_salida', 'Cliente_Disp': 'cliente_disp',
                    'Reservacion': 'reservacion', 'h_can_res': 'can_res', 'h_cod_reserva': 'cod_reserva',
                    'h_edo': 'estado', 'h_codigop': 'codigo_postal', 'h_correo_e': 'correo',
                    'h_nom': 'nombre', 'h_tfa_total': 'tfa_total', 'moneda_cve': 'moneda',
                    'h_ult_cam_fec_okt': 'ultimo_cambio','Tipo_Habitacion_nombre_split': 'tipo_habitacion'}
    reservaciones.rename(columns=rename_cols, inplace=True)




        # Replace values containing "CONVENCION SANFER" with "CONVENCION SANFER"
    reservaciones['nombre'] = reservaciones['nombre'].apply(
        lambda x: 'CONVENCION SANFER' if 'CONVENCION SANFER' in str(x) else x
)

    reservaciones_dashboard = reservaciones.copy()

    # number of persons must be greater than 0

    # group by nombre and estado to get a unique key

    # Crear características
    features_dashboard = reservaciones.groupby(['client_key', 'nombre']).agg(
        num_visits=('fecha_llegada', 'nunique'),
        #avg_days_between_visits=('h_res_fec_okt', lambda x: (x.diff().mean().days if x.count() > 1 else 0)),
        last_visit=('fecha_llegada', 'max'),
        last_reservation=('fecha_reservacion', 'max'),
        avg_dias_estancia=('num_noches','mean'),
        dias_estancia_total=('num_noches','sum'),
        total_rooms_reserved=('num_habitaciones', 'sum'),
        avg_rooms_reserved=('num_habitaciones', 'mean'),
        avg_expense=('tfa_total', 'mean'),
        total_expense=('tfa_total', 'sum'),
        total_people_stayed=('num_personas', 'sum'),
        avg_people_stayed=('num_personas', 'mean'),
        total_adults=('num_adultos', 'sum'),
        avg_adults=('num_adultos', 'mean'),
        num_reservations=('id_reserva', 'nunique'),
        total_children=('num_menores', 'sum'),
        avg_children=('num_menores', 'mean'),
        origin_country = ('pais', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        segmento_comp=('segmento', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        most_common_canal=('canal', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        most_common_agency = ('agencia', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        most_common_package=('paquete', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        most_common_room_type=('tipo_habitacion', lambda x: x.mode().iloc[0] if not x.mode().empty else None)
    ).reset_index()

    features_dashboard['empresa'] = 'HOTEL 1'
    features_dashboard['time_since_last_res'] = max_date-features_dashboard['last_reservation']
    features_dashboard['time_since_last_res'] = features_dashboard['time_since_last_res'].dt.days
    features_dashboard['churn'] = features_dashboard['time_since_last_res'] > 365
    features_dashboard.rename(columns={'client_key': 'id_cliente'}, inplace=True)   
    features_dashboard.rename(columns={'last_reservation': 'fecha_reservacion'}, inplace=True)   


    return reservaciones_dashboard, features_dashboard, features_model
