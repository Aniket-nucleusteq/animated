from email import header
from unicodedata import name
from pyspark import SparkConf, SparkContext
from pyspark import SQLContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.functions import date_format
from pyspark.sql.functions import current_timestamp

if __name__ == "__main__":
    conf = SparkConf().setAppName("Spark_Example")
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

#schema = StructType([ \
 #  StructField("id",IntegerType(),True), \

  #  StructField("name",StringType(),True), \
   # StructField("TimeStamp",StringType(),True)
  #])  from this you can give the schema to the csv file if not given in csv file

def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr+x[0:1].upper()+x[1:len(x)] + " "
    return resStr 



def convertDate(str):
    resStr1=""
    arr = str.split(" ")
    for x in arr:
        resStr1=resStr1+x.replace('/','-')+" "
        break
    return resStr1

def convertTimeFormat(str):
    resStr1=""
    arr = str.split(" ")
    for x in arr:
        resStr1=x.replace('/','-')+" "
    return resStr1



convertUDF=udf(lambda x:convertCase(x))     #function is registered as udf
convertUDF1=udf(lambda x:convertTimeFormat(x))
convertUDF2=udf(lambda x:convertDate(x))

info=sqlContext.read.csv("hdfs:///user/maria_dev/Demo/data_transformation.csv",header=True,inferSchema=True)
#.info.show()               #created a dataframe
info1=info.select("id",convertUDF("name").alias("Name"),convertUDF1("timestamp").alias("Time"),convertUDF2("timestamp").alias("Date"),"timestamp").withColumn('current', date_format(current_timestamp(), 'yyyy-MM-dd')).show()
#info1.write.csv("/user/maria_dev/Demo/Output",header=True)  #To save the content as a file

#o/p:-
#+---+------+------+-----------+-----------------+----------+
#| id|  Name|  Time|       Date|        timestamp|   current|
#+---+------+------+-----------+-----------------+----------+
#|  1|Test1 |10:14 |01-03-2022 |01/03/2022  10:14|2022-03-23|
#|  2|Test2 |10:14 |01-04-2022 |01/04/2022  10:14|2022-03-23|
#|  3|Test3 |10:14 |01-02-2022 |01/02/2022  10:14|2022-03-23|
#|  4|Test4 |10:14 |01-04-2022 |01/04/2022  10:14|2022-03-23|
#|  5|Test5 |10:14 |01-04-2022 |01/04/2022  10:14|2022-03-23|
#+---+------+------+-----------+-----------------+----------+


#rdd.saveAsTextFile(outputFile)