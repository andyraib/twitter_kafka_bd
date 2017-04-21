import pymongo as mon
from kafka import KafkaConsumer
import json


#Seleccionar el consumidor que se utilizará 'tomongo'
consumer = KafkaConsumer('tomongo')

#Es el mongo que será ocupado para mandar la información de twitter con su respectivo puerto
mongoClient = mon.MongoClient('54.174.5.92',27017)

# Conexión a la base de datos
db = mongoClient.andreadbtw

# Selecciona la coleccion
andreatw = db.andreatw

#Es el elasticsearch que será ocupado para mandar la información de twitter con su respectivo puerto
for msg in consumer:
	
	#Se guarda el valor del mensaje en una variable 'raw' en un formato utf-8
	raw = msg.value.decode('utf8')

	#Los valores obtenido se guardarán en un JSON. 
	doc = json.loads(raw)
	
	#Por cada raw guardado en el doc, se imprimirá en consola los entities, user_mentions y el name
	#Si el raw no es guardado correctamente, mandará un mensaje que no se encontro usuario
	try:
		print("Guardando: "+str(doc['entities']['user_mentions'][0]['name']))
	
	except:
		print('No se encontro nombre usuario')
         
	#Se guardará el documento en la collección ya seleccionada
	res = andreatw.insert_one(doc)