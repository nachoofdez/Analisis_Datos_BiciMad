'''
Programa para hacer gráficas a partir de los archivos de texto
obrenidos del estudio previo.
'''

import matplotlib.pyplot as plt

def obtener_grafica(namefile, pos, title, xlabel, ylabel): #pos indica la posición del primer dato al hacer split de una línea del fichero
    datos = []
    f = open(namefile, 'r')
    for linea in f:
        print(linea)
        linea_split = linea.split()
        if linea_split != []:
            datos.append((float(linea_split[pos]), float(linea_split[-1])))
    datos = sorted(datos, key=lambda x: x[0]) #Ordenamos los datos respecto de la x
    x, y = unzip(datos)
    
    plt.plot(x, y)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.show

def unzip(datos):
    x = []
    y = []
    for d in datos:
        x.append(d[0])
        y.append(d[1])
    return x, y
        
