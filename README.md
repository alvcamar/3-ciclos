# 3-ciclos
entrega de la practica de 3-ciclos programación paralela UCM

En este github se encuentran 3 archivos Python y 7 archivos con extension de archivo de texto plano.
Los 7 archivos, son ejemplos que se han visto en clase que representan las aristas de un grafo, por ejemplo:
  Si en un archivo encontramos A,B significa que del nodo A al nodo B, existe una arista que los une.
  
En lo relativo a los archivos Python:
  El archivo que resuelve el apartado 1, para ejecutarse en la terminal, se tiene que poner el nombre del archivo.py seguido del 
  archivo de texto del grafo que queramos obtener sus 3-ciclos. Por ejemplo: python3 3-ciclos_apartado1.py g2.txt
  En este archivo, se crea un rdd con todas las aristas que son distintas del grafo que le hayamos introducido (funcion vista en clase)
  A continuacion, tal y como se nos sugiere en la pista dada, creamos un rdd de pares 8ordenados por claves) de manera que la clave son
  los nodos del grafo pero los valores son los nodos adyacentes al valor de la clave (sin repetir. Es decir, si tenemos la arista 'A,B',
  en los valores de 'A' estará 'B' pero en los de 'B' NO estará 'A').
  Una vez tenemos hecho este rdd, para cada nodo, aplicamosmla funcion lista_asociada, que es la que crea nodos 'exists' y nodos 'pending'
  de la segunda parte de la pista. Y hacemos un flatMap para que el resultado nos devuelva un rdd en forma de una única lista. Finalmente, agrupamos por clave (nodos iguales)
  Para terminar, los triciclos se obtienen filtrando los valores de la lista anterior (pues es una lista de pares) con los elementos cuya segunda componente s tenga, al menos 2 elementos y que encuentre la palabra 'exists' en esa segunda componente (la segunda componente es una lista). Después los agrupamos por ternas y nos queda un rdd final que al aplicarle un collect(), nos sale una lista de ternas, que es lo que queríamos, pues estas ternas representan los 3-ciclos del grafo.
  
  En el archivo que resuelve el apartado 2, suponemos que tenemos que encontrar los 3-ciclos de un grafo que se encuentra escrito
  en distintos archivos de texto. Inicialmente, para ejecutarlo, se haría de la siguiente manera: python3 3-ciclos_apartado2.py g2.txt g4.txt g6.txt ... y podemos poner todos los archivos que queramos en fila.
  Para resolver el apartado 2, los nombres de los archivos se meten en una lista, luego se paralelizan y se crea un rdd que aplica la funcion "abrir el fichero en modo lectura" a cada uno de los archivos de manera paralela usando un flatMap. Así obtenemos un rdd con el contenido de cada fichero.
  Una vez tenemos esto hecho, seleccionamos los nodos, cogemos los grafos que no tengan aristas que vayan de un nodo en sí mismo con un 'filter' y finalmente, cogemos todos los pares que son distintos. Teniendo ya todo esto, copiamos el codigo del apartado 1 y ya lo tenemos resuelto.
  
  Para resolver el apartado 3, suponemos que tenemos varios grafos repartidos en varios ficheros, pero cada grafo de cada fichero es independiente con cualquier otro, y queremos encontrar los 3-ciclos de cada grafo en paralelo. Para ejecutar este archivo, la manera de proceder es igual a la del apartado 2, cambiando '3-ciclos_apartado1.py' por '3-ciclos_apartado3.py'.
  El funcionamiento es muy similar: se ha modificado la funcion 'get_edges' para que ahora devuelva una tupla de tuplas con el nombre del fichero correspondiente. Así, al ejecutarse, se va a crear un rdd vacío y, seguidamente, para cada fichero de la lista, lo vamos a leer, nos vamos a quedar con los nodos que son distintos (y sin repetir aristas y guardandonos el nombre del archivo correspondiente) y luego cogemos las que son distintas.
  Una vez tenemos esto, creamos el rdd union con el contenido de cada archivo. Cuando ya tenemos esto, repetimos el procediemiento del apartado 1, teniendo en cuenta que ahora también tenemos en la tupla el nombre del fichero del cual hemos leído la arista.
