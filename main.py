import datetime
import hadoopMR
import kafkaTopic
import streamingData

#Tweets
date = datetime.datetime.now().date();
print("Start BD: " + str(date))

numero = -1
while ( numero != 0):
    print("-----------------------------------------------------------------------")
    print("Introduzca un número para ejecutar una de las siguientes operaciones:")
    print("1. Streaming API")
    print("2. HadoopMR")
    print("3. kafkaStreaming (Necesario arrancar Consumer Streaming)")
    print("4. kafkaBatch (Necesario arrancar Consumer Batch)")
    print("5. Cassandra")
    print("6. MongoDB")
    print("-----------------------------------------------------------------------")

    numero = int(input())  # Pedimos numero al usuario
    if (numero == 1):
        streamingData.streamingAPI()
    elif (numero == 2):
        hadoopMR.mapPart()
    elif (numero == 3):
        streamingData.streamingAPI()
    elif (numero == 4):
        kafkaTopic.sendKafkaBatch()
    else:
        print("Número incorrecto")




