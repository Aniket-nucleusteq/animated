from email import header
from pydoc import describe
from unicodedata import name
from pyspark import SparkConf, SparkContext
from pyspark import SQLContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.functions import date_format
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import to_timestamp

if __name__ == "__main__":
    conf = SparkConf().setAppName("Spark_Example")
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr+x[0:1].upper()+x[1:len(x)] + " "
    return resStr 

convertUDF=udf(lambda x:convertCase(x))     #function is registered as udf

info=sqlContext.read.csv("hdfs:///user/maria_dev/Demo/data_transformation.csv",header=True,inferSchema=True)
#.info.show()               #created a dataframe
info1=info.select("id",convertUDF("name").alias("Name"),to_timestamp("timestamp","mm-dd-yy HH:mm").alias("Timestamp")).withColumn('current', (current_timestamp())).orderBy("Timestamp",ascending=False).dropDuplicates(["Name"]).orderBy("id",ascending=True).coalesce(1).write.csv("/user/maria_dev/Demo/Output",header=True)

info1=sqlContext.read.csv("hdfs:///user/maria_dev/Demo/Output",header=True,inferSchema=True)
info1.printSchema()
#o/p
#|-- id: integer (nullable = true)
#|-- Name: string (nullable = true)
#|-- Timestamp: timestamp (nullable = true)
#|-- current: timestamp (nullable = true)

info1.show()
#info2.printSchema()
#o/p:-
#+---+-----+-------------------+--------------------+
#| id| Name|          Timestamp|             current|
#+---+-----+-------------------+--------------------+
#|  2|Test2|2022-01-04 10:14:00|2022-03-25 04:15:...|
#|  3|Test3|2022-01-02 10:14:00|2022-03-25 04:15:...|
#|  4|Test4|2022-01-04 10:14:00|2022-03-25 04:15:...|
#|  5|Test5|2022-01-04 10:14:00|2022-03-25 04:15:...|
#|  6|Test1|2022-01-05 10:14:00|2022-03-25 04:15:...|
#+---+-----+-------------------+--------------------+
