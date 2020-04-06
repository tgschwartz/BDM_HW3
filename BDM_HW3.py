from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as func
import sys

sc = SparkContext()
spark = SparkSession(sc)
sc
rdd = sc.textFile(sys.argv[1]).cache()

def extractData(partId, records):
    if partId == 0:
        next(records)
        next(records)
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        year = row[0].split('-')[0]
        product = row[1].lower()
        issue = row[3]
        company = row[7]
        yield (product, year, company, issue)
        
Data = rdd.mapPartitionsWithIndex(extractData)
num_complaints = Data.toDF().groupby('_1', '_2', '_3').count().groupby('_1', '_2').max().orderBy('_1')

Data = rdd.mapPartitionsWithIndex(extractData)
unique_complaints = Data.toDF().groupby('_1', '_2').agg(func.count('_4'), func.countDistinct('_4')).orderBy('_1')

final_df = unique_complaints.join(num_complaints, ['_1', '_2']) \
    .withColumn("percent", ((func.col("max(count)") / func.col("count(_4)"))*100).cast('integer')) \
    .drop("max(count)").orderBy('_1')

return final_df.write.csv(sys.argv[2])