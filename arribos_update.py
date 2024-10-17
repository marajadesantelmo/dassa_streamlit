#!/usr/bin/python
# -*- coding: latin-1 -*-
'''
Script que actualiza los googlesheets para tablero Orden del dia
'''

#%% Setting
import pyodbc
import pandas as pd
import os
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage
import time

if os.path.exists('//dc01/Usuarios/PowerBI/flastra/Documents/automatizaciones'):
    os.chdir('//dc01/Usuarios/PowerBI/flastra/Documents/automatizaciones')
else:
    print("Se usa working directory por defecto")
# CONEXION SQL
print('Descargando datos de SQL')
server = '101.44.8.58\\SQLEXPRESS_X86,1436'
username = 'dassa'
password = 'Da$$a3065!'
conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';UID='+username+';PWD='+ password)
cursor = conn.cursor()

fecha = datetime.now().strftime('%Y-%m-%d')
fecha_ant = datetime.now() - timedelta(days=120)
fecha_ant = fecha_ant.strftime('%Y-%m-%d')
fecha_ant_ult3dias = datetime.now() - timedelta(days=3)
fecha_ant_ult3dias = fecha_ant_ult3dias.strftime('%Y-%m-%d')

#%% Arribos

#Descargo contenedores IMPO a arribar
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

#Descargo Contendores Ingresados
cursor.execute(f"""
    SELECT orden_ing, suborden, renglon, fecha_ing, tipo_oper, contenedor FROM DEPOFIS.DASSA.[Ingresadas En Stock]
    WHERE fecha_ing BETWEEN '{fecha_ant}' AND '{fecha}'
    AND tipo_oper = 'IMPORTACION'
    AND suborden= 0
""")  
rows = cursor.fetchall()
columns = [column[0] for column in cursor.description]
ingresos = pd.DataFrame.from_records(rows, columns=columns)

#Descargo Contendores Desconsolidados
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

#Descargo Contendores Consolidados
cursor.execute(f"""
    SELECT orden_ing, suborden, renglon, contenedor 
    FROM DEPOFIS.DASSA.[Ingresadas En Stock]
    WHERE fecha_ing BETWEEN '{fecha_ant}' AND '{fecha}'
    AND tipo_oper = 'EXPORTACION'
    AND suborden != 0 
""")  
rows = cursor.fetchall()
columns = [column[0] for column in cursor.description]
consolidados = pd.DataFrame.from_records(rows, columns=columns)

###### Descargo listos para remitir
cursor.execute("""
 SELECT orden_ing, contenedor, conocim, cliente, bookings, tipo_cnt, dimension, fecha_ing 
 FROM DEPOFIS.DASSA.[Existente En Stock]
 WHERE tipo_oper = 'EXPORTACION'
 AND tipo_trans != 'MERCADERIA'
 AND suborden = 0    
""")  # Agregar filtro de consolidoados

rows = cursor.fetchall()
columns = [column[0] for column in cursor.description]
contenedores_existente_expo = pd.DataFrame.from_records(rows, columns=columns)


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
contenedores_existente_impo = pd.DataFrame.from_records(rows, columns=columns)

#Pendiente consolidar
cursor.execute("""
    SELECT orden_ing, suborden, renglon, contenedor, fecha_ing, tipo_oper, bookings, cantidad, kilos, volumen, cliente 
    FROM DEPOFIS.DASSA.[Existente En Stock]
    WHERE tipo_oper = 'EXPORTACION'
    AND tipo_trans = 'MERCADERIA'
    AND suborden = 0
""")  
rows = cursor.fetchall()
columns = [column[0] for column in cursor.description]
pendiente_consolidar = pd.DataFrame.from_records(rows, columns=columns)

#Descargo contenedores IMPO a arribar y a desconsolidar en los ultimos dias
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
    SELECT orden_ing, suborden, renglon, cliente, agencia, fecha_ing, contenedor, conocim1, desc_merc, dimension, tipo_cnt, volumen, kilos
    FROM [DEPOFIS].[DASSA].[Existente en Stock]
    WHERE tipo_oper = 'IMPORTACION' 
""")  
rows = cursor.fetchall()
columns = [column[0] for column in cursor.description]
existente_impo = pd.DataFrame.from_records(rows, columns=columns)

#Descargo Ubicaciones IMPO
cursor.execute("""
    SELECT orden_ing, suborden, renglon, ubicacion
    FROM [DEPOFIS].[DASSA].[Ubic_St]
""")  
rows = cursor.fetchall()
columns = [column[0] for column in cursor.description]
ubicaciones_existente = pd.DataFrame.from_records(rows, columns=columns)

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


conn.close()

#%% Formateo y procesamiento de datos Arribos
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
    df['operacion'] = df['operacion'].str.strip()
    return df

def crear_operacion(df): 
    df['operacion'] = (df['orden_ing'].astype(str) + '-' + df['suborden'].astype(str) + '-' + df['renglon'].astype(str))
    return(df)

# IMPO a arribar
arribos = transformar(arribos)
arribos['Turno2'] = arribos['turno'].str.strip().apply(lambda x: x[:2] + ":" + x[2:] + ":00" if x.strip() else pd.NaT)
arribos['Fecha y Hora'] = pd.to_datetime(arribos['fecha']) + pd.to_timedelta(arribos['Turno2'])
current_time = pd.Timestamp.now()
arribos['tiempo_transcurrido'] = current_time - arribos['Fecha y Hora']
arribos['tiempo_transcurrido'] = arribos['tiempo_transcurrido'].apply(lambda x: '-' if x.days < 0 else '{:02}:{:02}'.format(x.seconds // 3600, (x.seconds // 60) % 60))
arribos['cliente'] =  arribos['cliente'].str.title()
arribos['buque'] =  arribos['buque'].str.title()

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
contenedores_existente_impo['contenedor'] = contenedores_existente_impo['contenedor'].str.strip()
contenedores_existente_impo= contenedores_existente_impo[contenedores_existente_impo['contenedor'] != '']
arribados_a_desconsolidar= arribados_a_desconsolidar[arribados_a_desconsolidar['operacion'] == 'TD']
arribados_a_desconsolidar= arribados_a_desconsolidar[arribados_a_desconsolidar['arribado'] == 1]
arribados_a_desconsolidar['Entrega'] = arribados_a_desconsolidar['Entrega'].str.title()
desconsolidados = desconsolidados['contenedor'].unique()
pendiente_desconsolidar = arribados_a_desconsolidar[arribados_a_desconsolidar['contenedor']!=""]
pendiente_desconsolidar['Estado'] = 'Pte. Desc.'
pendiente_desconsolidar.loc[pendiente_desconsolidar['contenedor'].isin(desconsolidados), 'Estado'] = 'Vacio'
pendiente_desconsolidar = pendiente_desconsolidar[~pendiente_desconsolidar.isin(desco_egresados)]
contenedores_existente = contenedores_existente_impo['contenedor'].unique()
pendiente_desconsolidar = pendiente_desconsolidar[pendiente_desconsolidar['contenedor'].isin(contenedores_existente)]
pendiente_desconsolidar = pendiente_desconsolidar.merge(
    contenedores_existente_impo[['contenedor', 'cliente', 'cantidad']],
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

##Existente EXPO##
#Pendiente de consolidar
pendiente_consolidar['contenedor'] = pendiente_consolidar['contenedor'].str.strip()
pendiente_consolidar['cliente'] = pendiente_consolidar['cliente'].str.strip()
pendiente_consolidar['cliente'] = pendiente_consolidar['cliente'].str.title()
fecha_actual = datetime.strptime(fecha, '%Y-%m-%d')
pendiente_consolidar['fecha_ing'] = pd.to_datetime(pendiente_consolidar['fecha_ing'], format='%Y-%m-%d')
pendiente_consolidar['Dias'] = (fecha_actual - pendiente_consolidar['fecha_ing']).dt.days

#Pendiente de remitir
consolidados['contenedor'] = consolidados['contenedor'].str.strip()
consolidados = consolidados['contenedor'].unique()
listos_para_remitir = contenedores_existente_expo[contenedores_existente_expo['contenedor'].isin(consolidados)]
vacios_disponibles = contenedores_existente_expo[~contenedores_existente_expo['contenedor'].isin(consolidados)]
vacios_disponibles['cliente'] = vacios_disponibles['cliente'].str.title()
listos_para_remitir['cliente'] = listos_para_remitir['cliente'].str.title()

listos_para_remitir['fecha_ing'] = pd.to_datetime(listos_para_remitir['fecha_ing'], format='%Y-%m-%d')
listos_para_remitir['Dias'] = (fecha_actual - listos_para_remitir['fecha_ing']).dt.days

#Existente IMPO
existente_impo['contenedor'] = existente_impo['contenedor'].str.strip()
existente_impo = crear_operacion(existente_impo)
ubicaciones_existente = crear_operacion(ubicaciones_existente)
ubicaciones_existente['ubicacion'] = ubicaciones_existente['ubicacion'].str.strip()
existente_impo = pd.merge(existente_impo, ubicaciones_existente[['operacion', 'ubicacion']], on='operacion', how='left')
familias_ubicaciones = pd.read_excel('flias_ubicaciones.xlsx')
existente_impo = pd.merge(existente_impo, familias_ubicaciones[['ubicacion', 'ubicacion_familia']], on='ubicacion', how='left')
existente_impo['teus'] = existente_impo['dimension']/20
existente_plz = existente_impo[existente_impo['ubicacion_familia'].isin(['Plazoleta', 'Temporal'])]
existente_alm = existente_impo[~existente_impo['ubicacion_familia'].isin(['Plazoleta', 'Temporal'])]


# Creo variable estado en Arribos IMPO
arribos['Estado'] = arribos['arribado'].apply(lambda x: 'Pendiente arribo' if x == 0 else 'Arribado')
arribos.loc[arribos['contenedor'].isin(pendiente_ingresado['contenedor']), 'Estado'] = 'Pendiente ingreso'
arribos.loc[arribos['Estado'] == 'Arribado', 'tiempo_transcurrido'] = '---'

arribos_historico_horarios = pd.read_csv('arribos_historico_horarios.csv')
arribos = pd.merge(arribos, arribos_historico_horarios[['contenedor', 'fecha', 'estado']], on=['contenedor', 'fecha'], how='left')
arribos['Estado'] = arribos['estado'].fillna(arribos['Estado'])
arribos = arribos.drop(columns=['estado'])

arribos_recientes = arribos[arribos['Estado']=='Arribado']
arribos_recientes['estado'] = datetime.now().strftime('%H:%M') + ' Arribado' 
arribos_recientes = arribos_recientes[['fecha', 'contenedor', 'estado']]
arribos_historico_horarios = pd.concat([arribos_historico_horarios, arribos_recientes], ignore_index=True)
arribos_historico_horarios.to_csv('arribos_historico_horarios.csv', index=False)


#%% Update Google Sheets
print('Cargando a GoogleSheets')
gc = gspread.service_account(filename='credenciales_gsheets.json')
google_sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1r66h7BCAu-CyG5uRsITYhuds9uGlw7k_-Xu24WNX43Q')

sheet = google_sheet.get_worksheet(0)
sheet.clear()
if arribos.empty:
    empty_arribos = pd.DataFrame({'contenedor': ['No hay arribos para hoy', 'No hay arribos para hoy'],
                                   'buque': [' ', ' '],
                                   'terminal': [' ', ' '],
                                   'fecha': ['1999-01-01', '1999-01-01'],
                                   'turno': ['0100', '0100'],
                                   'peso': [0, 0],
                                   'operacion': ['T', 'TD'],
                                   'arribado': [1, 0],
                                   'dimension': [0, 0],
                                   'tipo_cnt': [' ', ' '],
                                   'despachant': [' ', ' '],
                                   'precinto': [' ', ' '],
                                   'bookings': [' ', ' '],
                                   'cliente': [' ', ' '],
                                   'Turno2':  ['00:00:00', '00:00:00'], 
                                   'Fecha y Hora': ['1999-01-01 12:00', '1999-01-01 21:00'],
                                   'tiempo_transcurrido': ['1:00', '1:00'],
                                   'Estado': [' ', ' '],
                                   })
    set_with_dataframe(sheet, empty_arribos)
else: 
    set_with_dataframe(sheet, arribos)
time.sleep(30)
print('Esperando un minuto para seguir subiendo informacion')
sheet2 = google_sheet.get_worksheet(1)
sheet2.clear()
if arribos_expo.empty:
    empty_arribos_expo = pd.DataFrame({'orden' : [' ', ' '],
                                    'contenedor': ['No hay arribos para hoy', 'No hay arribos para hoy'],
                                   'buque': [' ', ' '],
                                   'terminal': [' ', ' '],
                                   'fecha': ['1999-01-01', '1999-01-01'],
                                   'turno': ['1200', '1200'],
                                   'peso': [0, 0],
                                   'operacion': ['T', 'TD'],
                                   'arribado': [1, 0],
                                   'chapa_trac': [' ', ' '],
                                   'bookings': [' ', ' '],
                                   'tipo_oper': [' ', ' '],
                                   'cliente': [' ', ' '],
                                   'desc_merc': [' ', ' '],
                                   'precinto' : [' ', ' '],
                                   'dimension':  [0, 0],
                                   'Contenedor2': ['No hay arribos para hoy', 'No hay arribos para hoy'], 
                                   })
    set_with_dataframe(sheet2, empty_arribos_expo)
else: 
    set_with_dataframe(sheet2, arribos_expo)

sheet3 = google_sheet.get_worksheet(2)
sheet3.clear()

if pendiente_ingresado.empty:
    empty_ingresado = pd.DataFrame({'contenedor': ['No hay pendientes', 'No hay pendientes'],
                                   'buque': [' ', ' '],
                                   'terminal': [' ', ' '],
                                   'fecha': ['1999-01-01', '1999-01-01'],
                                   'turno': ['1200', '1200'],
                                   'peso': [0, 0],
                                   'operacion': ['T', 'TD'],
                                   'arribado': [1, 0],
                                   'Contenedor2': ['No hay arribos para hoy', 'No hay arribos para hoy'],
                                   'Turno2': [' ', ' ']})
    set_with_dataframe(sheet3, empty_ingresado)
else: 
    set_with_dataframe(sheet3, pendiente_ingresado)
    
sheet4 = google_sheet.get_worksheet(3)
sheet4.clear()
if pendiente_desconsolidar.empty:
    empty_pendiente_desconsolidar = pd.DataFrame({'contenedor': ['No hay pendientes', 'No hay pendientes'],
                                   'buque': [' ', ' '],
                                   'terminal': [' ', ' '],
                                   'fecha': ['1999-01-01', '1999-01-01'],
                                   'turno': ['1200', '1200'],
                                   'peso': [0, 0],
                                   'operacion': ['T', 'TD'],
                                   'arribado': [1, 0],
                                   'dimension': [0, 0], 
                                   'tipo_cnt': [' ', ' '],
                                   'vto_vacio': ['1999-01-01', '1999-01-01'], 
                                   'Entrega': ['  ', '  '],
                                   'Estado': ['  ', '  '],
                                   'cliente' : [' ', ' '], 
                                   'cantidad' : [' ', ' '] })
    set_with_dataframe(sheet4, empty_pendiente_desconsolidar)
else: 
    set_with_dataframe(sheet4, pendiente_desconsolidar)

sheet5 = google_sheet.get_worksheet(4)
sheet5.clear()
if pendiente_consolidar.empty:
    empty_pendiente_consolidar = pd.DataFrame({'contenedor': ['No hay pendientes', 'No hay pendientes'],
                                   'buque': [' ', ' '],
                                   'terminal': [' ', ' '],
                                   'fecha': ['1999-01-01', '1999-01-01'],
                                   'turno': ['1200', '1200'],
                                   'peso': [0, 0],
                                   'operacion': ['T', 'TD'],
                                   'arribado': [1, 0],
                                   'dimension': [0, 0]})
    set_with_dataframe(sheet5, empty_pendiente_consolidar)
else: 
    set_with_dataframe(sheet5, pendiente_consolidar)
    
time.sleep(90)
print('Esperando un minuto para seguir subiendo informacion')

sheet6 = google_sheet.get_worksheet(5)
sheet6.clear()
if listos_para_remitir.empty:
    empty_listos_para_remitir = pd.DataFrame({
                                    'orden_ing': ['111', '111'],
                                    'contenedor': ['No hay pendientes', 'No hay pendientes'],
                                    'conocim': [' ', ' '],
                                    'cliente': [' ', ' '],
                                    'bookings': [' ', ' '],
                                    'tipo_cnt': [' ', ' '],
                                    'dimension': [0, 0],
                                    'fecha_ing': ['1999-01-01', '1999-01-01'],
                                    'Dias': [0, 0]})
    set_with_dataframe(sheet6, empty_listos_para_remitir)
else: 
    set_with_dataframe(sheet6, listos_para_remitir)

sheet7 = google_sheet.get_worksheet(6)
sheet7.clear()
if vacios_disponibles.empty:
    empty_vacios_disponibles = pd.DataFrame({'contenedor': ['No hay pendientes', 'No hay pendientes'],
                                   'buque': [' ', ' '],
                                   'terminal': [' ', ' '],
                                   'fecha': ['1999-01-01', '1999-01-01'],
                                   'turno': ['1200', '1200'],
                                   'peso': [0, 0],
                                   'operacion': ['T', 'TD'],
                                   'arribado': [1, 0],
                                   'dimension': [0, 0]})
    set_with_dataframe(sheet7, vacios_disponibles)
else: 
    set_with_dataframe(sheet7, vacios_disponibles)

sheet8 = google_sheet.get_worksheet(7)
sheet8.clear()
set_with_dataframe(sheet8, existente_plz)

sheet9 = google_sheet.get_worksheet(8)
sheet9.clear()
set_with_dataframe(sheet9, existente_alm)


arribos.to_csv('arribos/arribos.csv', index=False)
arribos_expo.to_csv('arribos/arribos_expo.csv', index=False)
pendiente_ingresado.to_csv('arribos/pendiente_ingresado.csv', index=False)
pendiente_desconsolidar.to_csv('arribos/pendiente_desconsolidar.csv', index=False)
pendiente_consolidar.to_csv('arribos/pendiente_consolidar.csv', index=False)
listos_para_remitir.to_csv('arribos/listos_para_remitir.csv', index=False)
vacios_disponibles.to_csv('arribos/vacios_disponibles.csv', index=False)
existente_plz.to_csv('arribos/existente_plz.csv', index=False)
existente_alm.to_csv('arribos/existente_alm.csv', index=False)
#%% Envio de alertas por mail

arribos_historico = pd.read_csv('arribos_historico.csv')
current_time = pd.Timestamp.now()
ayer_medianoche = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
arribos['time_difference'] = current_time - arribos['Fecha y Hora']
alertas_a_enviar = arribos[arribos['Fecha y Hora'] > ayer_medianoche]
alertas_a_enviar = alertas_a_enviar[alertas_a_enviar['arribado']==0]
alertas_a_enviar = alertas_a_enviar[alertas_a_enviar['time_difference'] > pd.Timedelta(hours=4)]
alertas_a_enviar = alertas_a_enviar[~alertas_a_enviar['contenedor'].isin(arribos_historico['contenedor'])]     
alertas_a_enviar = alertas_a_enviar[['contenedor', 'buque', 'terminal', 'fecha', 'turno']]

if not alertas_a_enviar.empty:
    # Convierto el dataframe para imprimirlo en el mail
    alertas_a_enviar_str =alertas_a_enviar.to_string(index=False)

    # Compose the email
    body = f"Ya pasaron 4hs desde el turno de este contenedor: \n\n  {alertas_a_enviar_str}"
    message = EmailMessage()
    message['From'] = "auto@dassa.com.ar"
    message['To'] = "alan@dassa.com.ar, christian@dassa.com.ar, marcos@dassa.com.ar, gabriel@dassa.com.ar, marajadesantelmo@gmail.com, auto@dassa.com.ar"
    # message['To'] = "marajadesantelmo@gmail.com"
    message['Subject'] = "ALERTA: 4hs transcurridas desde turno de contenedor"
    message.set_content(body)

    # Send the email using Gmail's SMTP server
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login("auto@dassa.com.ar", "gyctvgzuwfgvmlfu")
    server.send_message(message)
    print(f"Se enviaron alertas por 4hs desde turno de contenedor: \n\n{alertas_a_enviar}")
    print("Guardando buques nuevos con fecha forzoso en nuestra base")
    arribos_historico = pd.concat([arribos_historico, alertas_a_enviar[['fecha', 'contenedor']]], ignore_index=True)
    arribos_historico.to_csv('arribos_historico.csv', index=False)
else:
    print("No se enviaron alertas ")                    
                         
#%%Logeo

sheet_logs =  gc.open_by_url('https://docs.google.com/spreadsheets/d/1aPUkhige3tq7_HuJezTYA1Ko7BWZ4D4W0sZJtsTyq3A')                                           
worksheet_logs = sheet_logs.worksheet('Logeos')
df_logs = worksheet_logs.get_all_values()
df_logs = pd.DataFrame(df_logs[1:], columns=df_logs[0])
now = datetime.now().strftime('%Y-%m-%d %H:%M')
new_log_entry = pd.DataFrame([{'Rutina': 'Arribos', 'Fecha y Hora': now}])
df_logs = pd.concat([df_logs, new_log_entry], ignore_index=True)
worksheet_logs.clear()
set_with_dataframe(worksheet_logs, df_logs)
print("Se registr� el logeo")    


                         
                         