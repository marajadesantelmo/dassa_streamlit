import pandas as pd
from tokens import username, password

def highlight(row):
    if "Realizado" in row['Estado']:
        return ['background-color: darkgreen; color: black' for _ in row]
    elif "En curso" in row['Estado']:
        return ['background-color: darkgoldenrod; color: black' for _ in row]
    elif row['Estado'] == "Vacio":
        return ['background-color: #be1e2d; color: black' for _ in row]
    elif "Arribado" in row['Estado']:
        return ['background-color: darkgreen; color: black' for _ in row]
    elif row['Estado'] == "Pendiente ingreso":
        return ['background-color: darkgoldenrod; color: black' for _ in row]
    else:
        return ['' for _ in row]

def rellenar_df_vacio(df):
    if df.empty:
        df = pd.DataFrame([['-'] * len(df.columns)], columns=df.columns)
    return df

def fetch_data(username, password):
    import pyodbc
    import pandas as pd
    import os
    from datetime import datetime, timedelta
    from tokens import username, password
    if os.path.exists('//dc01/Usuarios/PowerBI/flastra/Documents/dassa_streamlit'):
        os.chdir('//dc01/Usuarios/PowerBI/flastra/Documents/dassa_streamlit')
    elif os.path.exists('C:/Users/facun/OneDrive/Documentos/GitHub/dassa_streamlit'):
        os.chdir('C:/Users/facun/OneDrive/Documentos/GitHub/dassa_streamlit')
    else:
        print("Se usa working directory por defecto")
    # CONEXION SQL
    print('Actualizando información operativa Orden del Día DASSA')
    print('Descargando datos de SQL')
    server = '101.44.8.58\\SQLEXPRESS_X86,1436'
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    fecha = datetime.now().strftime('%Y-%m-%d')
    fecha_ant = datetime.now() - timedelta(days=30)
    fecha_ant = fecha_ant.strftime('%Y-%m-%d')
    fecha_ant_ult3dias = datetime.now() - timedelta(days=3)
    fecha_ant_ult3dias = fecha_ant_ult3dias.strftime('%Y-%m-%d')
    def limpiar_columnas(df):
        columns = ['cliente', 'tipo_oper', 'desc_merc', 'Envase']
        for column in columns:
            if column in df.columns:
                df[column] = df[column].str.strip()
                df[column] = df[column].str.title()
        return df

    #Contenedores IMPO a arribar
    cursor.execute(f"""
        SELECT c.contenedor, c.buque, c.terminal, c.fecha, c.turno, c.peso, c.operacion, 
            c.arribado, c.dimension, c.tipo_cnt, c.despachant, c.precinto, c.bookings, 
            cl.apellido AS cliente
        FROM [DEPOFIS].[DASSA].[CORDICAR] c
        JOIN DEPOFIS.DASSA.[Clientes] cl ON c.cliente = cl.clie_nro
        WHERE c.tipo_oper = 'IMPORTACION' 
        AND c.fecha = '{fecha}'
    """)   
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    arribos = pd.DataFrame.from_records(rows, columns=columns)
    #Contendores Ingresados
    cursor.execute(f"""
        SELECT orden_ing, suborden, renglon, fecha_ing, tipo_oper, contenedor FROM DEPOFIS.DASSA.[Ingresadas En Stock]
        WHERE fecha_ing BETWEEN '{fecha_ant}' AND '{fecha}'
        AND tipo_oper = 'IMPORTACION'
        AND suborden= 0
    """)  
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    ingresos = pd.DataFrame.from_records(rows, columns=columns)
    #Contendores Desconsolidados
    cursor.execute(f"""
        SELECT orden_ing, suborden, renglon, contenedor, fecha_ing, tipo_oper 
        FROM DEPOFIS.DASSA.[Ingresadas En Stock]
        WHERE fecha_ing BETWEEN '{fecha_ant}' AND '{fecha}'
        AND tipo_oper = 'IMPORTACION'
        AND suborden!= 0
    """)  
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    desconsolidados = pd.DataFrame.from_records(rows, columns=columns)
    #Contendores Existente IMPO
    cursor.execute(f"""
        SELECT e.orden_ing, e.suborden, e.renglon, e.contenedor, e.fecha_ing, e.tipo_oper, 
            e.vto_vacio, e.cliente, e.cantidad, env.detalle AS Envase
        FROM DEPOFIS.DASSA.[Existente En Stock] e
        JOIN DEPOFIS.DASSA.[Tip_env] env ON e.tipo_env = env.codigo
        WHERE e.fecha_ing BETWEEN '{fecha_ant}' AND '{fecha}'
        AND e.tipo_oper = 'IMPORTACION'
        AND e.suborden = 0
    """)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    contenedores_existente = pd.DataFrame.from_records(rows, columns=columns)
    #Contenedores IMPO a arribar y a desconsolidar en los ultimos dias
    cursor.execute(f"""
        SELECT 
            c.contenedor, c.buque, c.terminal, c.fecha, c.turno, c.peso, 
            c.operacion, c.arribado, c.dimension, c.tipo_cnt, c.vto_vacio, 
            d.detalle AS Entrega
        FROM [DEPOFIS].[DASSA].[CORDICAR] c
        LEFT JOIN [DEPOFIS].[DASSA].[destinos] d ON c.term_ent = d.codigo
        WHERE c.tipo_oper = 'IMPORTACION' 
        AND c.fecha BETWEEN '{fecha_ant}' AND '{fecha}'
    """) 
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    arribados_a_desconsolidar = pd.DataFrame.from_records(rows, columns=columns)
    #Descargo Existente IMPO
    cursor.execute("""
        SELECT e.orden_ing, e.suborden, e.renglon, e.cliente, e.tipo_oper, e.fecha_ing, 
        e.contenedor, e.conocim1, e.desc_merc, e.dimension, e.tipo_cnt, e.volumen, env.detalle AS Envase, 
        e.cantidad, e.conocim2, e.kilos, e.bookings, e.precinto
        FROM [DEPOFIS].[DASSA].[Existente en Stock] e
        JOIN DEPOFIS.DASSA.[Tip_env] env ON e.tipo_env = env.codigo
        WHERE e.tipo_oper = 'IMPORTACION' 
    """)  
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    existente = pd.DataFrame.from_records(rows, columns=columns)
    #Ubicaciones
    cursor.execute("""
        SELECT orden_ing, suborden, renglon, ubicacion
        FROM [DEPOFIS].[DASSA].[Ubic_St]
    """)  
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    ubicaciones_existente = pd.DataFrame.from_records(rows, columns=columns)
    #Ubicaciones del egresado
    cursor.execute(f"""
    SELECT  orden_ing, suborden, renglon, ubicacion
    FROM [DEPOFIS].[DASSA].[Egresadas del stock]
    WHERE fecha_egr > '{fecha_ant}'
    """) 
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    ubicaciones_egresado = pd.DataFrame.from_records(rows, columns=columns)
    ubicaciones = pd.concat([ubicaciones_existente, ubicaciones_egresado], ignore_index=True)
    #Egresado 
    cursor.execute(f"""
    SELECT  e.orden_ing, e.suborden, e.renglon, e.cliente, e.tipo_oper, e.fecha_ing, 
    e.contenedor, e.desc_merc, e.conocim AS conocim1, e.dimension, e.tipo_cnt, e.volumen, env.detalle AS Envase
    FROM [DEPOFIS].[DASSA].[Egresadas del stock] e
    JOIN DEPOFIS.DASSA.[Tip_env] env ON e.tipo_env = env.codigo
    WHERE e.fecha_egr > '{fecha_ant}'
    """) 
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    egresado = pd.DataFrame.from_records(rows, columns=columns)
    egresado['id'] = egresado[['orden_ing', 'suborden', 'renglon']].astype(str).agg('-'.join, axis=1)
    #Tally
    cursor.execute("""
        SELECT t.tally, cl.apellido AS Cliente, bookings, contenedor, cantidad, kilos, volumen, dimension, env.detalle AS Envase
        FROM [DEPOFIS].[DASSA].[Tally] t
        JOIN DEPOFIS.DASSA.[Clientes] cl ON t.cliente = cl.clie_nro
        JOIN DEPOFIS.DASSA.[Tip_env] env ON t.tipo_env = env.codigo""")
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    tally = pd.DataFrame.from_records(rows, columns=columns)
    tally = tally.tail(100)
    #Verificaciones realizadas
    cursor.execute(f"""
        SELECT orden_ing, suborden, renglon, fechaverif            
        FROM [DEPOFIS].[DASSA].[Todo] 
        WHERE fechaverif > '{fecha_ant}'
    """)  
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    verificaciones_realizadas = pd.DataFrame.from_records(rows, columns=columns)
    #Salidas validadas
    cursor.execute(f"""
        SELECT orden_ing, suborden, renglon, validada          
        FROM [DEPOFIS].[DASSA].[Salidas] 
        WHERE validada > '{fecha_ant}'
    """)  
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    salidas = pd.DataFrame.from_records(rows, columns=columns)
        #Turnos
    cursor.execute(f"""
    SELECT orden_ing, suborden, renglon, destino, dia, hora, observa, conocim2
    FROM DEPOFIS.DASSA.[TURNOSSA] as e
    WHERE dia >= '{fecha}'
    """) 
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    turnos = pd.DataFrame.from_records(rows, columns=columns)
    turnos['destino'] = turnos['destino'].str.strip()
    turnos['Estado'] = 'Pendiente'
    turnos['observa'] = turnos['observa'].str.strip()
    turnos = turnos[turnos['orden_ing'] != 0]



    print('Procesando datos')

    def transformar(df):
        # Dimension
        df['dimension'] = pd.to_numeric(df['dimension'])
        df.loc[df['dimension'] == 20, 'dimension'] = 30
        df.loc[(df['dimension'] == 40) & (df['tipo_cnt'] == 'HC'), 'dimension'] = 70
        df.loc[(df['dimension'] == 40) & (df['tipo_cnt'] != 'HC'), 'dimension'] = 60
        df['dimension'] = df['dimension'].astype(int)
        df['dimension'] = df['dimension'].astype(str)
        # Terminal
        df['terminal'] = df['terminal'].astype(str)
        terminal_mapping = {
            '10057': 'TRP',
            '10073': 'Exolgan',
            '10068': 'Terminal 4'}
        df['terminal'] = df['terminal'].replace(terminal_mapping)
        # Contenedor
        df['contenedor'] = df['contenedor'].str.strip()
        df = df[df['contenedor'] != '']
        # Operacion
        df = df.copy()
        df.loc[:, 'operacion'] = df['operacion'].str.strip()
        return df

    def crear_operacion(df): 
        df['id'] = (df['orden_ing'].astype(str) + '-' + df['suborden'].astype(str) + '-' + df['renglon'].astype(str))
        return(df)

    def generate_id(df):
        df['id'] = df[['orden_ing', 'suborden', 'renglon']].astype(str).agg('-'.join, axis=1)
        df.drop(columns=['orden_ing', 'suborden', 'renglon'], inplace=True)
        return df

    # IMPO a arribar
    arribos = transformar(arribos)
    arribos['Turno2'] = arribos['turno'].str.strip().apply(lambda x: x[:2] + ":" + x[2:] + ":00" if x.strip() else pd.NaT)
    arribos['Fecha y Hora'] = pd.to_datetime(arribos['fecha']) + pd.to_timedelta(arribos['Turno2'])
    current_time = pd.Timestamp.now()
    arribos['tiempo_transcurrido'] = current_time - arribos['Fecha y Hora']
    arribos['tiempo_transcurrido'] = arribos['tiempo_transcurrido'].apply(lambda x: '-' if x.days < 0 else '{:02}:{:02}'.format(x.seconds // 3600, (x.seconds // 60) % 60))
    arribos['cliente'] =  arribos['cliente'].str.title()
    arribos['buque'] =  arribos['buque'].str.title()
    #Contenedores ingresados
    ingresos['contenedor'] = ingresos['contenedor'].str.strip()
    arribados = arribos[arribos['arribado']==1]
    pendiente_ingresado=  arribados[~arribados['contenedor'].isin(ingresos['contenedor'])]
    arribados_a_desconsolidar = transformar(arribados_a_desconsolidar)
    #Pendientes desconsolidar y vacios
    egresado['tipo_oper'] = egresado['tipo_oper'].str.strip()
    egresado['contenedor'] = egresado['contenedor'].str.strip()
    desco_egresados = egresado[(egresado['suborden'] == 0) & (egresado['tipo_oper'] == 'IMPORTACION')]
    desco_egresados = desco_egresados[desco_egresados['contenedor']!=""]
    desco_egresados = desco_egresados['contenedor'].unique()
    contenedores_existente['contenedor'] = contenedores_existente['contenedor'].str.strip()
    contenedores_existente= contenedores_existente[contenedores_existente['contenedor'] != '']
    arribados_a_desconsolidar= arribados_a_desconsolidar[arribados_a_desconsolidar['operacion'] == 'TD']
    arribados_a_desconsolidar= arribados_a_desconsolidar[arribados_a_desconsolidar['arribado'] == 1]
    arribados_a_desconsolidar['Entrega'] = arribados_a_desconsolidar['Entrega'].str.title()
    desconsolidados = desconsolidados['contenedor'].unique()
    pendiente_desconsolidar = arribados_a_desconsolidar[arribados_a_desconsolidar['contenedor']!=""]
    pendiente_desconsolidar['Estado'] = 'Pte. Desc.'
    pendiente_desconsolidar.loc[pendiente_desconsolidar['contenedor'].isin(desconsolidados), 'Estado'] = 'Vacio'
    pendiente_desconsolidar = pendiente_desconsolidar[~pendiente_desconsolidar.isin(desco_egresados)]
    contenedores_existente_uniques = contenedores_existente['contenedor'].unique()
    pendiente_desconsolidar = pendiente_desconsolidar[pendiente_desconsolidar['contenedor'].isin(contenedores_existente_uniques)]
    pendiente_desconsolidar = pendiente_desconsolidar.merge(
        contenedores_existente[['contenedor', 'cliente', 'cantidad']],
        on='contenedor',
        how='left'
    )
    pendiente_desconsolidar['cliente'] = pendiente_desconsolidar['cliente'].str.title()
    pendiente_desconsolidar.drop(columns=['cantidad'], inplace=True)
    # Group by 'contenedor' and aggregate the required columns
    tally_resumen= tally.groupby('contenedor').agg({
        'cantidad': 'sum',
        'kilos': 'sum',
        'Envase': lambda x: ', '.join(x.unique())
    }).reset_index()
    tally_resumen['Envase'] = tally_resumen['Envase'].str.title()
    pendiente_desconsolidar = pendiente_desconsolidar.merge(tally_resumen, on='contenedor', how='left')
    #Existente IMPO
    existente['contenedor'] = existente['contenedor'].str.strip()
    existente = crear_operacion(existente)
    ubicaciones = crear_operacion(ubicaciones)
    ubicaciones['ubicacion'] = ubicaciones['ubicacion'].str.strip()
    existente = pd.merge(existente, ubicaciones[['id', 'ubicacion']], on='id', how='left')
    familias_ubicaciones = pd.read_excel('flias_ubicaciones.xlsx')
    existente = pd.merge(existente, familias_ubicaciones[['ubicacion', 'ubicacion_familia']], on='ubicacion', how='left')
    existente['teus'] = existente['dimension']/20
    existente_plz = existente[existente['ubicacion_familia'].isin(['Plazoleta', 'Temporal'])]
    existente_alm = existente[~existente['ubicacion_familia'].isin(['Plazoleta', 'Temporal'])]
    # Creo variable estado en Arribos IMPO
    arribos['Estado'] = arribos['arribado'].apply(lambda x: 'Pendiente arribo' if x == 0 else 'Arribado')
    arribos.loc[arribos['contenedor'].isin(pendiente_ingresado['contenedor']), 'Estado'] = 'Pendiente ingreso'
    arribos.loc[arribos['Estado'] == 'Arribado', 'tiempo_transcurrido'] = '---'


    ##### TURNOS #####
    turnos['id'] = turnos[['orden_ing', 'suborden', 'renglon']].astype(str).agg('-'.join, axis=1)
    existente['id'] = existente[['orden_ing', 'suborden', 'renglon']].astype(str).agg('-'.join, axis=1)
    egresado = generate_id(egresado)
    verificaciones_realizadas = generate_id(verificaciones_realizadas)
    salidas = generate_id(salidas)
    salidas.columns = ['salida_validada', 'id']
    ubicaciones = generate_id(ubicaciones)

    turnos = limpiar_columnas(turnos)
    existente = limpiar_columnas(existente)

    #Separo segun tipo de turnos 1. Consolidados 2. Retiros y remisiones 3. Verificaciones 4. Clasificaciones

    ## 1. Consolidados
    consolidados = turnos[turnos['destino'].str.contains('Consolidado', case=False, na=False)]
    existente_a_consolidar = pd.merge(consolidados, existente.drop(columns=['id', 'suborden', 'renglon', 'conocim2']), on='orden_ing', how='inner')
    contenedores_a_consolidar = existente_a_consolidar[existente_a_consolidar['Envase'] == 'Contenedor']
    mercaderia_a_consolidar = existente_a_consolidar[existente_a_consolidar['Envase'] != 'Contenedor']
    mercaderia_a_consolidar = mercaderia_a_consolidar.groupby('orden_ing').agg({
        'volumen': 'sum',
        'cantidad': 'sum',
        'kilos': 'sum'}).reset_index()
    contenedores_a_consolidar.drop(columns=['volumen', 'cantidad', 'kilos'], inplace=True)
    consolidados = pd.merge(contenedores_a_consolidar, mercaderia_a_consolidar, on='orden_ing', how='left')

    ### (Quito conocimiento del existente)
    existente.drop(columns=['conocim2', 'orden_ing', 'suborden', 'renglon'], inplace=True)

    ## 2. Retiros y remisiones
    retiros_remisiones = turnos[turnos['destino'].str.contains('Retiro|Remi', case=False, na=False)]
    retiros_remisiones_egr = pd.merge(retiros_remisiones, egresado, on='id', how='inner')
    retiros_remisiones_egr['Estado'] = 'En curso'
    retiros_remisiones_exist = pd.merge(retiros_remisiones, existente, on='id', how='inner')
    retiros_remisiones_exist['Estado'] = 'Pendiente'
    retiros_remisiones_exist = retiros_remisiones_exist[~retiros_remisiones_exist['id'].isin(retiros_remisiones_egr['id'])] #Se sacan casos de retiros parciales
    retiros_remisiones = pd.concat([retiros_remisiones_egr, retiros_remisiones_exist], ignore_index=True)
    retiros_remisiones = pd.merge(retiros_remisiones, salidas, on='id', how='left')
    retiros_remisiones['fecha_salida_validada'] = pd.to_datetime(retiros_remisiones['salida_validada'], errors='coerce').dt.date
    retiros_remisiones['salida_validada'] = retiros_remisiones.apply(
        lambda row: float('nan') if pd.notna(row['fecha_salida_validada']) and row['fecha_salida_validada'] < datetime.now().date() else row['salida_validada'],
        axis=1)
    retiros_remisiones.drop(columns=['fecha_salida_validada'], inplace=True)
    retiros_remisiones['Estado'] = retiros_remisiones.apply(
        lambda row: row['salida_validada'][11:16] + ' Realizado' if pd.notna(row['salida_validada']) else row['Estado'],
        axis=1)

    ## 3. Verificaciones
    verificaciones = turnos[turnos['destino'].str.contains('Verificacion', case=False, na=False)]
    verificaciones= pd.merge(verificaciones, verificaciones_realizadas, on='id', how='left')
    verificaciones['Estado'] = verificaciones['fechaverif'].apply(lambda x: 'Realizado' if pd.notna(x) else 'Pendiente')
    verificaciones_existente = pd.merge(verificaciones, existente, on='id', how='inner')
    verificaciones_egresado = pd.merge(verificaciones, egresado, on='id', how='inner')
    verificaciones_sin_dato = verificaciones[
        ~verificaciones['id'].isin(verificaciones_existente['id']) &
        ~verificaciones['id'].isin(verificaciones_egresado['id'])
    ]
    verificaciones = pd.concat([verificaciones_existente, verificaciones_egresado, verificaciones_sin_dato], ignore_index=True)

    ## 4. Clasificaciones
    clasificaciones = turnos[turnos['destino'].str.contains('Clasi', case=False, na=False)]
    clasificaciones_existente = pd.merge(clasificaciones, existente, on='id', how='inner')
    clasificaciones_egresado = pd.merge(clasificaciones, egresado, on='id', how='inner')
    clasificaciones_sin_dato = clasificaciones[~clasificaciones['id'].isin(clasificaciones_existente['id']) & ~clasificaciones['id'].isin(clasificaciones_egresado['id'])]
    clasificaciones = pd.concat([clasificaciones_existente, clasificaciones_egresado, clasificaciones_sin_dato], ignore_index=True)

    #Unifico turnos y le hago join de ubicaciones
    turnos = pd.concat([retiros_remisiones, verificaciones, consolidados], ignore_index=True)
 #   turnos = pd.merge(turnos, ubicaciones, on='id', how='left')
    turnos = limpiar_columnas(turnos)

    ##### ARRIBOS EXPO #####

    #Descargo EXPO a arribar
    cursor.execute(f"""
    SELECT c.orden, c.contenedor, c.buque, c.terminal, c.fecha, c.turno, c.peso, 
        c.operacion, c.arribado, c.chapa_trac, c.bookings,  c.tipo_oper, 
        cl.apellido AS cliente, c.desc_merc, c.precinto, c.dimension, c.hora_ing
    FROM [DEPOFIS].[DASSA].[CORDICAR] c
    JOIN DEPOFIS.DASSA.[Clientes] cl ON c.cliente = cl.clie_nro
    WHERE c.tipo_oper != 'IMPORTACION' 
    AND c.fecha >= '{fecha}'
    """)  
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    arribos_expo = pd.DataFrame.from_records(rows, columns=columns)
    conn.close()
    # EXPO a arribar
    arribos_expo['terminal'] = arribos_expo['terminal'].astype(str)
    terminal_mapping = {
        '10057': 'TRP',
        '10073': 'Exolgan',
        '10068': 'Terminal 4'}
    arribos_expo['terminal'] = arribos_expo['terminal'].replace(terminal_mapping)
    arribos_expo['contenedor'] = arribos_expo['contenedor'].str.strip()
    arribos_expo['operacion'] = arribos_expo['operacion'].str.strip()
    arribos_expo['tipo_oper'] = arribos_expo['tipo_oper'].str.strip()
    arribos_expo['Contenedor2'] = arribos_expo['contenedor'].str.strip()    # Ver para que contenedor2
    arribos_expo['cliente'] =  arribos_expo['cliente'].str.title()
    arribos_expo['Estado'] = arribos_expo['arribado'].replace({0: 'Pendiente', 1: 'Arribado'})
    arribos_expo.loc[arribos_expo['Estado'] == 'Arribado', 'Estado'] = arribos_expo['hora_ing'].astype(str) + ' Arribado'

    ##### Formateo #####
    arribos = arribos[['terminal', 'turno', 'contenedor', 'cliente', 'bookings', 'tipo_cnt', 'tiempo_transcurrido', 'Estado']]
    arribos.columns = ['Terminal', 'Turno', 'Contenedor', 'Cliente', 'Bookings', 'Tipo', 'Temp.', 'Estado']
    arribos['Cliente'] = arribos['Cliente'].apply(lambda x: x[:10] + "..." if len(x) > 10 else x)
    arribos['Bookings'] = arribos['Bookings'].apply(lambda x: x[:10] + "..." if len(x) > 10 else x)
    arribos['Turno'] = arribos['Turno'].apply(lambda x: f"{str(x)[:-2]}:{str(x)[-2:]}")

    pendiente_desconsolidar = pendiente_desconsolidar[['contenedor', 'cliente', 'Entrega', 'vto_vacio', 'tipo_cnt', 'peso','Estado']]
    pendiente_desconsolidar.columns = ['Contenedor', 'Cliente', 'Entrega', 'Vto. Vacio', 'Tipo', 'Peso', 'Estado']
    pendiente_desconsolidar['Entrega'] = pendiente_desconsolidar['Entrega'].fillna('-')
    pendiente_desconsolidar['Cliente'] = pendiente_desconsolidar['Cliente'].apply(lambda x: x[:10] + "..." if len(x) > 10 else x)

    turnos['cliente'] = turnos['cliente'].apply(lambda x: x[:10] + "..." if isinstance(x, str) and len(x) > 10 else x)
    turnos['desc_merc'] = turnos['desc_merc'].apply(lambda x: x[:10] + "..." if isinstance(x, str) and len(x) > 10 else x)
    turnos['ubicacion'] = turnos['ubicacion'].str.strip()

    verificaciones_impo = turnos[(turnos['tipo_oper'] == 'Importacion') & (turnos['destino'] == 'Verificacion')]
    verificaciones_impo = verificaciones_impo[['dia', 'cliente', 'desc_merc', 'contenedor', 'Envase', 'cantidad', 'ubicacion', 'Estado']]
    verificaciones_impo.columns = ['Dia', 'Cliente', 'Desc. Merc.', 'Contenedor', 'Envase', 'Cant.', 'Ubic.', 'Estado']

    retiros = turnos[(turnos['tipo_oper'] == 'Importacion') & (turnos['destino'] == 'Retiro')]
    retiros = retiros[['dia', 'cliente', 'conocim1', 'contenedor', 'Envase', 'cantidad', 'ubicacion', 'Estado']]
    retiros.columns = ['Dia', 'Cliente', 'Conocim.', 'Contenedor', 'Envase', 'Cant.', 'Ubic.', 'Estado']
    retiros['Conocim.'] = retiros['Conocim.'].str.strip()

    otros_impo = turnos[(turnos['tipo_oper'] == 'Importacion') & (~turnos['destino'].isin(['Retiro', 'Verificacion']))]
    otros_impo = otros_impo[['dia', 'hora', 'id', 'cliente', 'contenedor', 'Envase', 'cantidad', 'ubicacion', 'Estado']]
    otros_impo.columns = ['Dia', 'Hora', 'Operacion', 'Cliente', 'Contenedor', 'Envase', 'Cant.', 'Ubic.', 'Estado']

    # Se rellenan los dataframes vacios
    arribos = rellenar_df_vacio(arribos)
    pendiente_desconsolidar = rellenar_df_vacio(pendiente_desconsolidar)
    verificaciones_impo = rellenar_df_vacio(verificaciones_impo)
    retiros = rellenar_df_vacio(retiros)
    otros_impo = rellenar_df_vacio(otros_impo)

    arribos_expo_carga = arribos_expo[arribos_expo['tipo_oper'] != 'VACIO']
    arribos_expo_ctns = arribos_expo[arribos_expo['tipo_oper'] == 'VACIO']

    arribos_expo_carga = arribos_expo_carga[['fecha', 'bookings', 'cliente', 'desc_merc', 'Estado']]
    arribos_expo_carga.columns = ['Fecha', 'Bookings', 'Cliente', 'Desc. Merc.', 'Estado']

    arribos_expo_ctns = arribos_expo_ctns[['fecha', 'bookings', 'cliente', 'dimension', 'contenedor', 'precinto','Estado']]
    arribos_expo_ctns.columns = ['Fecha', 'Bookings', 'Cliente', 'Dimension', 'Contenedor', 'Precinto', 'Estado']   

    verificaciones_expo = turnos[(turnos['tipo_oper'] == 'Exportacion') & (turnos['destino'] == 'Verificacion')]
    verificaciones_expo = verificaciones_expo[['dia', 'cliente', 'desc_merc', 'contenedor', 'Envase', 'cantidad', 'ubicacion']]
    verificaciones_expo = rellenar_df_vacio(verificaciones_expo)
    verificaciones_expo.columns = ['Dia', 'Cliente', 'Desc. Merc.', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.']

    retiros = turnos[(turnos['tipo_oper'] == 'Exportacion') & (turnos['destino'] == 'Retiro')]
    retiros = retiros[['dia', 'cliente', 'conocim1', 'contenedor', 'Envase', 'cantidad', 'ubicacion', 'Estado']]
    retiros.columns = ['Dia', 'Cliente', 'Conocim.', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.', 'Estado']

    otros_expo = turnos[(turnos['tipo_oper'] == 'Exportacion') & (~turnos['destino'].isin(['Retiro', 'Verificacion']))]
    otros_expo = otros_expo[['dia', 'hora', 'id', 'cliente', 'contenedor', 'Envase', 'cantidad', 'ubicacion']]
    otros_expo.columns = ['Dia', 'Hora', 'Operacion', 'Cliente', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.']

    remisiones = turnos[turnos['destino'] == 'Remision']
    consolidados = turnos[turnos['destino'] == 'Consolidado']

    arribos_expo_carga = rellenar_df_vacio(arribos_expo_carga)
    arribos_expo_ctns = rellenar_df_vacio(arribos_expo_ctns)
    verificaciones_expo = rellenar_df_vacio(verificaciones_expo)
    retiros = rellenar_df_vacio(retiros)
    otros_expo = rellenar_df_vacio(otros_expo)
    remisiones = rellenar_df_vacio(remisiones)
    consolidados = rellenar_df_vacio(consolidados)
    print('Proceso terminado')
    return arribos, pendiente_desconsolidar, verificaciones_impo, retiros, otros_impo, arribos_expo_carga, arribos_expo_ctns, verificaciones_expo, retiros, otros_expo, remisiones, consolidados
