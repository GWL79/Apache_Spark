from pyspark import SparkConf, SparkContext
import collections
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)
lines = sc.textFile(r"C:\Users\wlgan\Apache_Spark\ml-100k\ml-100k\u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
print(result)
sorted_seult = collections.OrderedDict(sorted(result.items()))
for key, value in sorted_seult.items():
    print(f"{key}, {value}")