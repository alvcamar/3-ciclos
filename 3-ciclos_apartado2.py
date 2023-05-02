#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: alvarocamarafernandez
"""

"""
Apartado 2:
Considera que los datos, es decir, la lista de las aristas, no se encuentran en un único fichero
sino en muchos.
Escribe un programa paralelo que calcule los 3-ciclos de un grafo que se encuentra definido
en múltiples ficheros de entrada.
"""

import sys
from pyspark import SparkContext

def get_edges(line): # grafo no dirigido, sin bucles.
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2


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


def main(sc, ficheros): #la idea es leer los ficheros en paralelo para que todos estén escribiendo su contenido en un fichero llamado "grafo_union.txt" y aplicar el apartado 1.
    
    ficheros_rdd = sc.parallelize(ficheros) #creamos un rdd de los ficheros
    contenido_rdd = ficheros_rdd.flatMap(lambda fichero: open(fichero, "r")) #rdd con el contenido de cada fichero
    aristas_grafo_rdd = contenido_rdd.\
        map(get_edges).\
        filter(lambda x: x is not None).\
        distinct()
    
    #copiamos el codigo que ya tenemos del ejercicio 1 y ya lo tenemos hecho.-
    
    lista_adyacentes = aristas_grafo_rdd.groupByKey().\
        map(convertir_valor_a_lista).\
        sortByKey().\
        map(ordenar_values) #pista 1 hecha: creamos una lista de pares (los pares ordenados por clave) cuyo valor es la lista de vertices adyacentes (sin repetir aristas)
    
    #lista de adyacencia de nodos posteriores (y ordenada lexicograficamente) creada (y sin repeticion de aristas).
    
    #print(lista_adyacentes.collect())
    
    lista = lista_adyacentes.flatMap(lista_asociada).\
        groupByKey() #lista de nodos 'exists' y nodos 'pending' agrupados por claves iguales
    
    #print(lista.collect())
    
    lista_triciclos = lista.map(convertir_valor_a_lista).\
        filter(condicion_filter).\
        flatMap(genera_ternas)
    
    print()
    print("------------------------------ RESULTADOS ------------------------------")
    print("Cantidad de triciclos encontrados en el grafo conjunto: ", lista_triciclos.count())
    print(lista_triciclos.collect())
    return lista_triciclos.collect()

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Uso: python3 {0} <filename 1> <filename 2> <filename 3> ··· <filename n>".format(sys.argv[0]))
    else:
        lst = [sys.argv[i] for i in range(1,(len(sys.argv)))] #lista con los nombres de archivos
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, lst)
