# Analisis_Datos_BiciMad
Análisis datos usuarios del servicio de alquiler de bicicletas públicas en Madrid mediante el uso de Programación Paralela con pyspark.

El objetivo de este trabajo ha consistido en crear un programa Python con la librería pySpark para dar solución a diferentes problemas que se han planteado sobre el conjunto de datos BICIMAD. Los archivos de texto corresponden a los datos solución obtenidos tras haber ejecutado el programa bicimad_final.py en el clúster @wild.mat.ucm.es sobre los datos del año 2017 en los meses entre abril y diciembre incluidos.

SOBRE LOS ARCHIVOS:
- Trabajo_BICIMAD.pdf: PDF que consiste en la memoria del trabajo. En este se explica en qué consiste el conjunto de datos, los problemas que se plantean, explicación del código de bicimad_final.py y algunas conclusiones y gráficas de los datos obtenidos de los archivos de texto.
- bicimad_final.py: Programa con la librearía pySpark que resuelve diferentes problemas planteados sobre el conjunto de datos
BiciMad tratándolos como un RDD. Un ejemplo de ejecución en el clúster sería: python3 bicimad_final.py "2017" "`seq 4 12`" /public/bicimad.
- bicimad_final2.py: Es igual que el anterior con el añadido de que si no encuentra la ruta del archivo continúa el programa sin este.
- EstudioCompleto.tar.gz: es una carpeta comprimida con los archivos de texto del estudio realizado desde abril 2017 hasta junio 2018.
- Las imágenes muestran la ejecución en el clúster del estudio completo y el tiempo de ejecución.
- graficas_bicimad.py: Programa Python para hacer las gráficas que aparecen en la memoria a partir de los archivos de texto.
- desenganches_por_dia_por_estacion.txt, enganches_desenganches_por_estacion_por_dia.txt y enganches_por_dia_por_estacion corresponden a los datos obtenidos al estudiar el promedio de las frecuencias de enganche y desenganche de las bicis en las diferentes estaciones en función de los días de la semana.
- tiempo_medio_por_dia_semana.txt, tiempo_medio_por_edad.txt y tiempo_medio_viaje_por_edad.txt son los datos sobre el estudio de los tiempos medios de viaje en función de la edad, día de la semana y el propio viaje.
- usuarios_unicos_por_dia.txt y usuarios_unicos_por_mes.txt son los datos obtenidos sobre el estudio de la cantidad de usuarios únicos (es decir, sin contar si utilizan más de una vez BICIMAD en el mismo día) por día y por mes.
- viajes_mismo_origen_destino_por_estacion.txt corresponde a la cantidad total de usos de BICIMAD donde las estación de enganche y de desenganche han sido las mismas.

AUTORES:
- Ignacio Fernández Sánchez-Pascuala.
- Javier Castellano Soria.
