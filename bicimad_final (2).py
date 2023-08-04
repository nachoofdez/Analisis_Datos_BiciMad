'''
PRÁCTICA 4: ANÁLISIS DE DATOS DE BICIMAD

AUTORES: Ignacio Fernández Sánchez-Pascuala
         Javier Castellano Soria

Programa con la librearía pySpark que resuelve diferentes problemas planteados sobre el conjunto de datos
BiciMad con la ayuda del clúster @wild.mat.ucm.es. Los resultados son almacenados en distintos archivos de texto,
uno para cada problema.

Un ejemplo de ejecucióm en el clúster sería: python3 bicimad_final.py "2017" "`seq 4 12`" /public/bicimad
que correspondería a un estudio de los meses desde abril hasta diciembre incluidos del año 2017.
'''
from pyspark import SparkContext, SparkConf
import json
from datetime import datetime
import sys

#Creamos el SparkContext
conf = SparkConf().setAppName("Bicimad Estudios")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")      



# Definimos una función que dada una línea del archivo BICIMAD (formato JSON), obtenemos la información deseada. 
def datos(line):
    data = json.loads(line) #Pasamos a diccionario el string en formato JSON
    origen = data['idunplug_station'] #Estación de desenganche
    destino=data['idplug_station'] #Estación de enganche
    fecha = datetime.strptime(data['unplug_hourTime']['$date'], "%Y-%m-%dT%H:%M:%S.%f%z")
    mes=fecha.month
    weekday=fecha.weekday()
    tiempo = data['travel_time']
    edad = data['ageRange']
    codigo_usuario = data['user_day_code']
    return origen, destino, weekday, edad, tiempo, codigo_usuario, mes #origen es estación de desenganche y destino estación de enganche


def main(sc, years, months, datadir):
     rdd = sc.parallelize([])
     for y in years: #Juntamos en un rdd los archivos de cada mes de cada año especificado
         for m in months:
             filename = f"{datadir}/{y}{m:02}_movements.json"
             print(f"Adding {filename}")
             rdd = rdd.union(sc.textFile(filename))
             
     n_semanas = len(years)*len(months)*4 #Hallamos el número de semanas aproximado para obtener luego el promedio por día de la semana
     n_years = len(years) #Hallamos el número de años para luego hacer el promedio por mes
     rdd_base=rdd.map(datos) #Obtenemos el rdd de partida con el que trabajaremos cada problema
     
     
     #Estudio media usuarios unicos por dia de la semana y mes:
         
     #Estudio media usuarios unicos por dia de la semana
     usuarios_unicos_dias(rdd_base,n_semanas)
     
     #Estudio media usuarios unicos por mes
     usuarios_unicos_meses(rdd_base,n_years)
     
     
     
     #Estudio frecuencias:
         
     #Estudio, por cada dia de la semana, frecuencia de enganches y desenganches de cada estacion.
     estudio_frecuencias_dias(rdd_base, n_semanas)
     
     #Estudio frecuencia de enganches y desenganches de cada estacion dependiendo del dia de la semana.
     estudio_frecuencias_estacion(rdd_base, n_semanas)
     
     
     
     #Estudio tiempo medio:
    
     #Estudio tiempo medio de cada viaje por edad
     tiempo_medio_viaje_por_edad(rdd_base)
     
     #Estudio tiempo medio por dia de la semana
     tiempo_medio_por_dia_semana(rdd_base)
     
     #Estudio tiempo medio por rango de edad
     tiempo_medio_por_edad(rdd_base)
     
     
     
     #Estudio de viajes
     
     #Estudio de cantidad de viajes donde el enganche y desenganche se hace en la misma estación
     viajes_mismo_origen_destino_por_estación(rdd_base)
     

'''
ESTUDIO USUARIOS ÚNICOS
'''

def usuarios_unicos_dias(rdd_base,n_semanas):
    #calcula la media total de usuarios unicos por dia de la semana
    #El user_day_code cambia de codigo al cambiar de fecha
    '''
    EXPLICACIÓN CÓDIGO:
    Con el primer map seleccionamos los datos que queremos: weekday y user_day_code.
    Como no quiero contar los usuarios que repiten día hago .distinct(). Luego me quedo con los días de la semana (clave)
    y pongo un 1 como valor para hacer el conteo con reduceByKey(). Después divido entre el número de semanas
    para tener el promedio por día de la semana.
    '''
    rdd_usuarios_dias = rdd_base.map(lambda x: (x[2],x[5])).distinct()\
        .map(lambda x: (x[0],1)).reduceByKey(lambda a, b:a+b).map(lambda x: (x[0],x[1]/n_semanas))
    lista_usuarios_dias=rdd_usuarios_dias.collect()
    
    #GUARDO LOS DATOS EN UN ARCHIVO DE TEXTO
    f = open('usuarios_unicos_por_dia.txt', 'w')
    for (weekday, media) in lista_usuarios_dias:
        print(f'Día de la semana {weekday} ----> Media de usuarios únicos {media}')
        f.write(f'Día de la semana {weekday} ----> Media de usuarios únicos {media}\n')
    f.close()  
    
def usuarios_unicos_meses(rdd_base,n_years):
    #calcula la media total de usuarios unicos por mes
    #El user_day_code cambia de codigo al cambiar de fecha
    '''
    EXPLICACIÓN CÓDIGO:
    Igual que el anterior pero ahora por meses.
    '''
    rdd_usuarios_meses = rdd_base.map(lambda x: (x[6],x[5])).distinct()\
        .map(lambda x: (x[0],1)).reduceByKey(lambda a, b:a+b).map(lambda x: (x[0],x[1]/n_years))
    
    #GUARDO LOS DATOS EN UN ARCHIVO DE TEXTO
    f = open('usuarios_unicos_por_mes.txt', 'w')
    for (mes, media) in rdd_usuarios_meses.collect():
        print(f'Mes {mes} ----> Media de usuarios únicos {media}')
        f.write(f'Mes {mes} ----> Media de usuarios únicos {media}\n')
    f.close()

'''
ESTUDIO DE FRECUENCIAS DE ENGANCHES Y DESENGANCHES
'''
     
def estudio_frecuencias_dias(rdd_base,n_semanas):   
    '''
    EXPLICACIÓN CÓDIGO:
    Con el primer map seleccionamos los datos que queremos: weekday y origen. Aprovechamos para poner (weekday,origen)
    como clave y un 1 como valor. Después hacemos reduceByKey para obtener la frecuencia de desenganches.
    
    Luego hago un map para poner el dia de la semana como clave y el par (origen, promedio) como valor al haber
    dividido la frecuencia entre el número de semanas. Por último agrupo por dia de la semana y ordeno las estaciones y
    promedio de mayor a menor según el promedio.
    '''
    rdd_frec_origen = rdd_base.map(lambda x: ((x[2],x[0]), 1)).reduceByKey(lambda a, b: a + b)
    #Obtenemos una lista de la forma [((weekday,origen),frecuencia),....]

    rdd_orden_origen = rdd_frec_origen.map(lambda x: (x[0][0], (x[0][1], x[1]/n_semanas))) \
        .groupByKey().mapValues(lambda x: sorted(list(x), key=lambda y: y[1], reverse=True))
    
    #GUARDAMOS LOS DATOS EN UN ARCHIVO DE TEXTO
    f = open('desenganches_por_dia_por_estacion.txt', 'w')
    for (weekday, origen_frec) in rdd_orden_origen.collect():
        print(f"Día: {weekday}")
        f.write(f"Día: {weekday}\n")
        for (origen, frec) in origen_frec:
            print(f"Estación de desenganche: {origen} ---> Frecuencia: {frec}")
            f.write(f"Estación de desenganche: {origen} ---> Frecuencia: {frec}\n")
        print()
        f.write('\n\n')
    f.close()
    
    '''
    EXPLICACIÓN CÓDIGO:
    Análogo al anterior pero ahora con destino (ENGANCHES).
    '''
    # Calcular la frecuencia (ENGANCHES) de cada estación de destino por día
    rdd_frec_destino = rdd_base.map(lambda x: ((x[2],x[1]), 1)).reduceByKey(lambda a, b: a + b)
    #Obtenemos una lista de la forma [((weekday,destino),frecuencia),....]

    #Ordenamos las estaciones de destino con más frecuencia por día
    rdd_orden_destino = rdd_frec_destino.map(lambda x: (x[0][0], (x[0][1], x[1]/n_semanas))) \
        .groupByKey().mapValues(lambda x: sorted(list(x), key=lambda y: y[1], reverse=True))
        
    #GUARDAMOS LOS DATOS EN UN ARCHIVO DE TEXTO
    f = open('enganches_por_dia_por_estacion.txt', 'w')
    for (weekday, destino_frec) in rdd_orden_destino.collect():
        print(f"Día: {weekday}")
        f.write(f"Día: {weekday}\n")
        for (destino, frec) in destino_frec:
            print(f"Estación de enganche: {destino} ---> Frecuencia: {frec}")
            f.write(f"Estación de enganche: {destino} ---> Frecuencia: {frec}\n")
        print()
        f.write('\n\n')
    f.close()
    

def estudio_frecuencias_estacion(rdd_base, n_semanas):
    # Calculamos el número total de llegadas (enganches) por día y estación
    rdd_llegadas = rdd_base.map(lambda x: ((x[2], x[1]), 1)).reduceByKey(lambda a, b: a + b)

    # Calculamos el número total de salidas (desenganches) por día y estación
    rdd_salidas = rdd_base.map(lambda x: ((x[2], x[0]), 1)).reduceByKey(lambda a, b: a + b)
    
    # Unimos los RDD de salidas y llegadas por día y estación
    rdd_resultado = rdd_salidas.join(rdd_llegadas)

    #Por cada estación (clave) obtenemos una lista de tuplas cuyas componentes son día de la semana y una
    #tupla correspondiente a las frecuencias de enganche y desenganche
    rdd_frec_total = rdd_resultado.map(lambda x: (x[0][1], (x[0][0], (x[1][0]/n_semanas, x[1][1]/n_semanas)))).groupByKey()
    
    #GUARDAMOS LOS DATOS EN UN ARCHIVO DE TEXTO
    f = open("enganches_desenganches_por_estación_por_dia.txt", 'w')
    for (station, weekday_frec) in rdd_frec_total.collect():
        print(f"Estacion {station}:")
        f.write(f"Estacion {station}:\n")
        for (weekday, frec) in weekday_frec:
            (frec_origen, frec_destino)=frec
            print(f"Día {weekday} ----> Frec_desenganche: {frec_origen}, Frec_enganche: {frec_destino}")
            f.write(f"Día {weekday} ----> Frec_desenganche: {frec_origen}, Frec_enganche: {frec_destino}\n")
        f.write("\n\n")
    f.close()

'''
ESTUDIO DE TIEMPOS DE VIAJE
'''
def tiempo_medio_viaje_por_edad(rdd_base):
    '''
    EXPLICACIÓN CÓDIGO:
    Con el primer map seleccionamos los datos que nos interesan: origen, destino, edad y tiempo, dejando los tres primeros
    argunmentos como una tupla. Filtramos aquellos cuyo grupo de edad sea > 0, es decir, el grupo de edad es conocido.
    Despúes hago un map a los valores (tiempo) para ponerlo como tuplas con un 1 que hará de contador. Luego,
    hago un reduceByKey sumando los valores y un mapValues que coge el tiempo total y lo divide entre el contador obteniendo
    así el tiempo promedio. Después hacemos un map y un groupByKey para organizar los datos por tupla (origen, destino) (clave).
    Por último, ordeno los tiempos de cada viaje por grupo de edad.
    
    '''
    rdd_tiempo_medio_viaje_por_edad = rdd_base.map(lambda x: ((x[0], x[1], x[3]), x[4])).filter(lambda x: x[0][2]>0)\
        .mapValues(lambda x: (x,1)).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0]/x[1])\
        .map( lambda x: ( ((x[0][0],x[0][1]), (x[0][2], x[1])) )).groupByKey().mapValues(lambda x: sorted(list(x), key=lambda y: y[0]))
    
    #GUARDO LOS DATOS EN UN ARCHIVO DE TEXTO
    f = open("tiempo_medio_viaje_por_edad.txt", "w")
    for (viaje, edad_tiempo) in rdd_tiempo_medio_viaje_por_edad.collect():
        print(f'Origen {viaje[0]} - Destino {viaje[1]}')
        f.write(f'Origen {viaje[0]} - Destino {viaje[1]}\n')
        for (edad, tiempo) in edad_tiempo:
            print(f'Grupo de edad {edad} ----> Tiempo promedio: {tiempo}')
            f.write(f'Grupo de edad {edad} ----> Tiempo promedio: {tiempo}\n')
        f.write('\n\n')
        print()
    f.close()
    
def tiempo_medio_por_dia_semana(rdd_base):
    '''
    EXPLICACIÓN CÓDIGO:
    Con el primer map seleccionamos los datos que nos interesan: weekday y tiempo. Además aprovechamos para poner
    weekday como clave y la tupla (tiempo, 1) como valor. Hacemos un reduceByKey para obtener el tiempo total por
    día de la semana y número de viajes. Terminamos con un mapValues para hallar el promedio.
    '''
    rdd_tiempo_medio_por_dia = rdd_base.map(lambda x: (x[2], (x[4], 1)))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0]/x[1])
        
    #GUARDAMOS LOS DATOS EN UN FICHERO DE DATOS
    f = open("tiempo_medio_por_dia_semana.txt", "w")
    for (dia, tiempo) in sorted(list(rdd_tiempo_medio_por_dia.collect()), key=lambda y: y[0]):
        print(f'Día {dia} ----> Tiempo promedio: {tiempo}')
        f.write(f'Día {dia} ----> Tiempo promedio: {tiempo}\n')
    print()
    f.close()
        
def tiempo_medio_por_edad(rdd_base):
    '''
    EXPLICACIÓN CÓDIGO:
    Es análogo al anterior pero ahora por grupo de edades en vez de días de la semana.
    '''
    rdd_tiempo_medio_por_edad = rdd_base.map(lambda x: (x[3], (x[4], 1)))\
        .filter(lambda x: x[0] > 0)\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0]/x[1])
    
    #GUARDAMOS LOS DATOS EN UN FICHERO DE DATOS
    f = open("tiempo_medio_por_edad.txt", "w")
    for (edad, tiempo) in sorted(list(rdd_tiempo_medio_por_edad.collect()), key=lambda y: y[0]):
        print(f'Grupo de edad {edad} ----> Tiempo promedio: {tiempo}')
        f.write(f'Grupo de edad {edad} ----> Tiempo promedio: {tiempo}\n')
    print()
    f.close()


'''
ESTUDIO DE VIAJES
'''
def viajes_mismo_origen_destino_por_estación(rdd_base):
    '''
    EXPLICACIÓN CÓDIGO:
    Con el primer map seleccionamos los datos que nos interesan: origen y destino. Después con el filter
    nos quedamos solo con las tuplas que tienen origen y destino idénticos. Con el map nos quedamos
    con el número de estación y ponemos 1 como valor para contar el número de viajes por estación con
    el reduceByKey.
    '''
    rdd_viajes_mismo_origen_destino = rdd_base.map(lambda x: (x[0], x[1]))\
        .filter(lambda x: x[0] == x[1]).map(lambda x: (x[0], 1))\
        .reduceByKey(lambda x, y: x + y)
        
    #GUARDAMOS LOS DATOS EN UN FICHERO DE DATOS
    f = open("viajes_mismo_origen_destino_por_estacion.txt", "w")
    for (estacion, n_viajes) in sorted(list(rdd_viajes_mismo_origen_destino.collect()), key=lambda y: y[1], reverse=True):
        print(f'Número de estación {estacion} ----> Número de viajes con mismo origen y destino: {n_viajes}')
        f.write(f'Número de estación {estacion} ----> Número de viajes con mismo origen y destino: {n_viajes}\n')
    print()
    f.close()
    





if __name__=="__main__":
    if len(sys.argv)<2:
        years = [2021]
    else:
        years = list(map(int, sys.argv[1].split()))

    if len(sys.argv)<3:
        months = [3]
    else:
        months = list(map(int, sys.argv[2].split()))

    if len(sys.argv)<4:
        datadir = "."
    else:
        datadir = sys.argv[3]

    print(f"years: {years}")
    print(f"months: {months}")
    print(f"datadir: {datadir}")

    main(sc, years, months, datadir)