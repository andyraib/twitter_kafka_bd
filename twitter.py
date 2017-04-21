from kafka import KafkaProducer
import tweepy as tpy
import limpieza as lm
import conteo as contar
import json



#Las llaves de accesos de twitter
CONSUMER_KEY = 'V767QxnD4CJi9wbbRNpOJOXRo'
CONSUMER_SECRET = 'xLA3lBfcjkQp3c2rROi4D3Z7UYJB3ldHK7OUfTvXFKsnM9ca0D'
ACCESS_KEY = '829188008893104128-Svu03FdKQeu2F9ZQhKDTGVR3FFdLMWw'
ACCESS_SECRET = 'z2n4rGXpjqlB02eMakHttXGhhIyk9qDcJn5YFDnWU6wjG'


#Se define el class  del streamListener
class Listener(tpy.StreamListener):


    # Se crea una funcion para conocer el status del StreamListener
    def on_error(self, status_code):

        # Se manda un False si no se logra conectar de manera correcta el StreamLister
        if status_code == 420:
            return False

    # Se crea funcion para mandar los tweets publicados que se registran
    def on_data(self, data):
    
        # Preparando datos para mandar información a Elasticsearch
        # Se manda los datos con el formato que requerimos, en este caso con una separación de una ',' y  ':'
        raw = json.loads(data)
        data1 = json.dumps(raw, sort_keys =True, indent=4, separators=[",", ":"])
        print(raw['text'])

        #Preparando datos para mandar inforamación a Mongo 
        #Se manda el conteo  con un script desarrollado en conteo.py
        #Se manda los datos con el formato que requerimos
        raw['palabra'] = contar.maximo(str(raw['text']))
        del raw['text']
        data2 = json.dumps(raw, sort_keys =True, indent=4, separators=[",", ":"]) 
       
        #Para elesticsearch el productor se manda al topico 'toelastic'
        producer.send('toelastic', str.encode(data1))

        #Para mongo el productor se manda al topico 'tomongo'

        producer.send('tomongo', str.encode(data2))

    
        #subprocess.call(['./opt/kafka/bin/kafka-run-class.sh', 'org.apache.kafka.streams.examples.wordcount.WordCountDemo'])

        #except:
               # print("Error codif")



auth = tpy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.secure = True
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
api = tpy.API(auth)

print("¡¡Twitter!!")

#Start Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Connect to the stream
print("Twiiters:")
escucha  = Listener()
Stream = tpy.Stream(auth=api.auth, listener=escucha)
Stream.filter(track=['BBVA'])                                    







