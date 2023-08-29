import json
import datetime
import Files
import csv

#File to MR
nameFile = "Files/finalFile.bak"
file = open(nameFile,"r", encoding="utf8")


#MAP
def mapPart():
	print("Ejecutado Mapper")
	#Cabecera
	file.readline()

	# Creation File
	fileMapper = open("Files/hadoopMR/mapper.txt", "w+", encoding="utf8")

	#Por cada entrada emitimos la tipo y texto
	for linea in file:
		seccion = linea.strip()
		dateRealesed,id,tweetType,text = seccion.split(";",3)
		fileMapper.write(dateRealesed[0:10] + ";" + tweetType + "\n")
	fileMapper.close()
	file.close()
	reducePart()

#REDUCE
def reducePart():
	print("Ejecutado Reducer")

	# Creation File
	fileReduce = open("Files/hadoopMR/reducer.txt", "w+", encoding="utf8")
	fileM = open("Files/hadoopMR/mapper.txt", "r+", encoding="utf8")

	linea = None
	lista = []

	for claveValor in fileM:
		# Dividir la linea
		dateRealesed, tweetType = claveValor.split(";", 1)
		cleanType = tweetType.replace("\n","")

		if linea == None:
			linea = dateRealesed

		if linea == dateRealesed:
			if cleanType not in lista:
				lista.append(cleanType)

		else:  # fecha distinta puede darse el caso si son las 12
			# pasamos al siguiente dia
			fileReduce.write(linea + str(lista) + "\n")
			linea = dateRealesed
			lista.clear()
			lista.append(cleanType)

	fileReduce.write(dateRealesed + str(lista))
	fileReduce.close()
	#print(linea + "\t" + separador.join(lista))