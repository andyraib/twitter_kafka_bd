#Importar las librerias necesarias
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from datetime import datetime
import json

#Seleccionar el consumidor que se utilizará 'toelastic'
consumer = KafkaConsumer('toelastic')

#Es el elasticsearch que será ocupado para mandar la información de twitter con su respectivo puerto
es = Elasticsearch(["http://search-beevagrad-yzavdnk3vgybj33teqgucq7ray.us-east-1.es.amazonaws.com:80"])

#Imprimiendo....
print("Procesando información....")

#Por cada mensaje que exista en el consumidor ...
for msg in consumer:

	#Se guarda el valor del mensaje en una variable 'raw' en un formato utf-8
	raw = msg.value.decode('utf8')

	#Los valores obtenido se guardarán en un JSON. 
	doc = json.loads(raw)

	#try Si existe los campos 'entities','user_mentions', lo imprimirá en consola, sino hará el except
	try:
		print(doc['entities']['user_mentions'][0]['name'])
	except:
		print('El usuario no tiene algunos datos')
	
	# Se enviará a la conexión establecida con elastic.
	# Se indica el nombre del index, el tipo de documento y el el contenido
	# Se imprime el estado del mensaje ya sea que el mensaje ha sido enviado correctamente o existió un error al enviar
	try:
		res = es.index(index="andreatw", doc_type="mensajes", body=doc)
		print("Status message: ",res['created'])
	except:
		print('!¡¡¡Error !!')	
