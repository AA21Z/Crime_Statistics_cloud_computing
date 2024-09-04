#l/usr/bin/env 
python from csv import reader
from pyspark import SparkContext
from datetime import datetime #import numpy as np

sc = SparkContext(appName="MySparkProg")
sc.setLogLevel("ERROR")
data = sc.textFile("hdfs://10.56.2.216:54310/hw1-input/*)

splitdata = data.mapPartitions(lambda x: reader(x))

splitdata = splitdata.filter(lambda x: x[7])
getheader = splitdata.first

city = splitdata.map(lambda x: x[13])
maxcrimecity = city.map(lambda x: (x,1)).reduceByKey(lambda a,b: a
+b).sortBy(ascending=False, keyfunc= lambda x:[1]). take(1)

julycrimesdata = splitdata.filter(lambda x: x!=getheader).filter(lambda x: datetime.strptime(x(5],
'%m/%d/%Y').month == 7)

julycrimesdesc = julycrimesdata.map(lambda x: x(7])

topcrimesdesc = julycrimesdesc.map(lambda crime: (crime, 1)).reduceByKey(lambda
dweaponcount = julycrimesdata.filter(lambda x: x7] == 'DANGEROUS WEAPONS').count

print("City in which most of crimes happening:, And number of crimes reproted in this city:")
print(maxcrimecity)

print("Top 3 crimes reported in the July month and their count:")
for crime in topcrimesdesc:
  print (crime)
  
print("Total count reported for Dangerous weapons crime type:")
print(dweaponcount)
