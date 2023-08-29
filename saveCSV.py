import json
import datetime
import os.path
import shutil

date = datetime.datetime.now().date();
nameFile = "Files/tweets_from_" + str(date) + ".csv"
file = open(nameFile, "w", encoding="utf-8")

def createCSV():
    if os.path.exists(nameFile):
        backup = nameFile + '.bak'
        shutil.copy(nameFile,backup)
        print(f'Se crea una copia de seguridad en {backup}.')
    else:
        with open(nameFile, "w", encoding="utf-8") as archivo:
            print(f'Creado archivo {nameFile}.')

    # Cabecera
    file.write("Date;Id;Type;Text;Tags" + "\n")

def data_tweets(jsonData,date):
    tweetInfo = json.loads(jsonData)
    id = tweetInfo['data']['id']
    text = tweetInfo['data']['text']
    text_filtered = text.replace("\n","")

    #Try extract type of Tweet
    tweetType = "Empty"
    try:
        tweetType = tweetInfo["data"]["referenced_tweets"][0]["type"]
    except:
        print("Referenced is empty...")

    # Try extract tags of Tweet
    tags = "Empty"
    try:
        tags = tweetInfo['matching_rules'][0]["tag"]
    except:
        print("Tags is empty...")

    #Tweet
    file.write(str(date) + ";" + str(id) + ";" + str(tweetType) + ";" + str(text_filtered) + ";" + str(tags) + "\n")

def closeCSV():
    file.close()