#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: alvarocamarafernandez
"""

"""
Apartado 3:
Supongamos que los datos del grafo se encuentran repartidos en múltiples ficheros.
 Queremos calcular los 3-ciclos, pero sólamente aquellos que sean locales 
 a cada uno de los ficheros.
Escribe un programa paralelo que calcule independientemente
los 3-ciclos de cada uno de los ficheros de entrada.
"""

import sys
from pyspark import SparkContext

def modif_get_edges(line,filename):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return ((n1,filename),(n2,filename))
    elif n1 > n2:
         return ((n2,filename),(n1,filename))
     #else: n1 == n2 -> devolvemos 'nada'

def convertir_valor_a_lista (tupla): # 'clave' es la primera componente de la tupla y 'valor' es la segunda
    return tupla[0], list(tupla[1])

def ordenar_values(tupla): #ordena la 2 componente de 'tupla' de menor a mayor
    return tupla[0], sorted(tupla[1])

def lista_asociada(tupla): #vamos a asociar la lista que está en el ejemplo de la hoja de los enunciados
    res = []
    nodo_actual = tupla[0]
    long = len(tupla[1])
    for i in range(long):
        res.append(((nodo_actual, tupla[1][i]), 'exists'))
        for j in range(i+1, long):
            res.append(((tupla[1][i],tupla[1][j]),('pending',nodo_actual)))
    return res #la lista todavia no está ordenada como sale en la 'pista'
    
def condicion_filter(tupla): # seleccionamos si tenemos una arista que se ha repetido de otro vertice, y además, si en esa lista hay un 'exists'.
    return (len(tupla[1])>= 2 and 'exists' in tupla[1])

def genera_ternas(tupla): #simplemente es para devolver el resultado en forma de lista de tuplas de 3 elementos
    res = []
    for elem in tupla[1]:
        if elem != 'exists':
            nodo_restante = elem[1]
            tupla_aux = (tupla[0][0], tupla[0][1], nodo_restante)
            res.append(tupla_aux)
    return res

def escribe_bonito(tupla):
    return [(tupla[0][0], tupla[1][0], tupla[2][0])]

def main(sc, files):
    rdd_contenido = sc.emptyRDD()
    for file in files:
        #para cada archivo, lo leemos -> cogemos cada linea del archivo (map) y nos quedamos con las aristas (y el nombre del archivo)
        #filtramos todas las aristas que no vayan de un vertice a si mismas y eliminamos las que estén duplicadas.
        file_rdd = sc.textFile(file).map(lambda a : modif_get_edges(a,file)).\
            filter(lambda x: x is not None).distinct()
        
        #lo agregamos a un rdd que contendrá todos los archivos que son distintos
        rdd_contenido = rdd_contenido.union(file_rdd).distinct()

    # una vez que tenemos el rdd con el contenido, repetimos el apartado 1:
    
    lista_adyacentes = rdd_contenido.groupByKey().\
        map(convertir_valor_a_lista).\
        sortByKey().\
        map(ordenar_values)
    
    lista = lista_adyacentes.flatMap(lista_asociada).\
        groupByKey()
    
    lista_triciclos = lista.map(convertir_valor_a_lista).\
        filter(condicion_filter).\
        flatMap(genera_ternas)
    
    print()
    print("-------------------- RESULTADOS --------------------")
    for file in files:
        #cada tupla tiene 3 elementos de la forma: (('B', 'g6.txt'), ('D', 'g6.txt'), ('A', 'g6.txt')) -> filtramos por nombre de archivo y borramos el archivo para printear
        rdd_filtrado = lista_triciclos.filter(lambda tupla: tupla[0][1] == file).\
            flatMap(escribe_bonito)
        print("Cantidad de triciclos encontrados en el grafo ", file, ": ", lista_triciclos.count())
        print("lista de triciclos encontrados: ", rdd_filtrado.collect())
        print()

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Uso: python3 {0} <filename 1> <filename 2> <filename 3> ··· <filename n>".format(sys.argv[0]))
    else:
        lst = [sys.argv[i] for i in range(1,(len(sys.argv)))] #lista con los nombres de archivos
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, lst)
