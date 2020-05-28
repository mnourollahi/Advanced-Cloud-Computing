import csv, json
import re

csvFilePath= '/home/masoum/input.csv'
jsonFilePath= '/home/masoum/input.json'

data= {}
i=1

with open(csvFilePath, encoding="utf8", errors='ignore') as csvFile:
	csvReader= csv.DictReader(csvFile)
	for rows in csvReader:	
		id= i
		i=i+1
		rows['id']=rows['id'][0:14]
		data[id]=rows
	
with open(jsonFilePath, 'wb') as jsonFile:
	jsonFile.write(json.dumps(data, ensure_ascii=False, indent= 4).encode('utf8'))

