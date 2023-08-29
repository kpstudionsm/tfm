#API
import datetime
import tweepy
import json
import time

import kafkaTopic
import saveCSV

#Claves
api_key='Poner Claves propias'
api_key_secret='Poner Claves propias'
consumer_key='Poner Claves propias'
consumer_secret='Poner Claves propias'
access_token='Poner Claves propias'
access_token_secret='Poner Claves propias'
bearer_token='Poner Claves propias'

client = tweepy.Client(bearer_token,api_key,api_key_secret,access_token,access_token_secret)
auth= tweepy.OAuthHandler(api_key,api_key_secret,access_token,access_token_secret)
api = tweepy.API(auth)

#Buscador de lo que contiene
search_terms = ["lunes","martes","spain"]

saveCSV.createCSV()

class MyStream(tweepy.StreamingClient):
    def in_connect(self):
        print("Connected")

    # def on_tweet(self, tweet):
    #     if tweet.referenced_tweets == None:
    #         save_tweets(testty)

    def on_errors(self, errors):
        print("Error")

    #Json
    def on_data(self, raw_data):
        time.sleep(0.5)
        date = datetime.datetime.now();
        saveCSV.data_tweets(raw_data,date)
        kafkaTopic.sendKafkaStreaming2(raw_data)
        print(raw_data)

    def on_disconnect(self):
        # End file
        saveCSV.closeCSV()
        print("Saved File")
        print("Disconnected")

    #Streaming response
    def on_response(self, response):
        print('Response: ' + str(response))

    def on_exception(self, exception):
        print("Exception text: " + str(exception))

def streamingAPI():
    stream = MyStream(bearer_token=bearer_token)

    #Buscador por termino
    for term in search_terms:
        stream.add_rules(tweepy.StreamRule(term))

    #Filtra los tweets correctamente
    stream.filter(tweet_fields=["referenced_tweets"])
