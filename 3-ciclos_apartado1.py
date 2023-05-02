#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: alvarocamarafernandez
"""

"""
Apartado 1:
    Los triciclos, o más formalmente 3-ciclos, 
    son un elemento básico en el análisis de grafos y subgrafos. 
    Los 3-ciclos muestran una relación estrecha entre 3 entidades (vértices): 
    cada uno de los vértices está relacionado (tiene aristas) con los otros dos.
    
Escribe un programa paralelo que calcule los 3-ciclos
de un grafo definido como lista de aristas.

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

def get_rdd_distict_edges(sc, filename):
    return sc.textFile(filename).\
        map(get_edges).\
        filter(lambda x: x is not None).\
        distinct()

def convertir_valor_a_lista (tupla): # 'clave' es la primera componente de la tupla y 'valor' es la segunda
    return tupla[0], list(tupla[1])

def ordenar_values(tupla): #ordena la 2 componente de 'tupla' de menor a mayor
    return tupla[0], sorted(tupla[1])

"""
def get_edges_rep(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 != n2:
         return [(n1,n2), (n2,n1)]
    else:
        return [(n1,n2)] #n1 == n2

def adjacents_2(sc, filename):
    edges = sc.textFile(filename).\
        flatMap(get_edges_rep).\
        filter(lambda x: x is not None).\
        distinct()
    adj = edges.groupByKey().collect()
    for node in adj:
        print(node[0], list(node[1]))
"""

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

def main(sc, filename):
    
    aristas_grafo_rdd = get_rdd_distict_edges(sc,filename) 
    
    #print(aristas_grafo_rdd.collect()) #lista de pares
    
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
    print("-------------------- RESULTADOS --------------------")
    print("Cantidad de triciclos encontrados en el grafo: ", lista_triciclos.count())
    print(lista_triciclos.collect())
    return lista_triciclos.collect()
    

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, sys.argv[1])
