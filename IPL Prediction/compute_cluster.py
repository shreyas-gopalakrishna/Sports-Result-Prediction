from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
import pickle

def format_input(input_file , indices) :
  tokenized = sc.textFile(input_file).flatMap(lambda line: line.split("\n"))
  stat = tokenized.map(lambda entry: (entry.split(",")[0] ,[float(entry.split(",")[indices[0]]),float(entry.split(",")[indices[1]]),float(entry.split(",")[indices[2]])]))
  data = tokenized.map(lambda entry: ([float(entry.split(",")[indices[0]]),float(entry.split(",")[indices[1]]),float(entry.split(",")[indices[2]])]))
  return [stat, data]

def cluster(filename, k, indices):
  stat, data = format_input("input/"+filename, indices)
  output = file("output/"+filename, "w") 
  model = KMeans.train(data, k)
  #print(model)
  #pickle.dump(model,open(filename+".p","wb"))
  
  P = dict()
  
  cluster_centers = model.clusterCenters
  with file("output/"+filename, "w") as f:
      for x in stat.collect():
        name = str(x[0])
        num = str(model.predict(x[1]))
        centers = str(' '.join('{:.3f}'.format(i) for i in cluster_centers[model.predict(x[1])]))
        
        P[name] = num
        f.write(name+","+num+"\n")
      pickle.dump(P,open(filename+".p","wb"))

if __name__ == "__main__":
  conf = SparkConf().setAppName("Spark Count")
  sc = SparkContext(conf=conf)
  cluster('batsmen', 8, [7, 9, 8])#ave,strike rate,balls faced
  cluster('bowler', 8, [11,10,4])#economy,balls,strike rate
  conf = SparkConf().setAppName('Hive')
  
  sqlContext = HiveContext(sc)
  
  sqlContext.sql("drop database IPL5 cascade");
  sqlContext.sql("create database IPL5")
  sqlContext.sql("use IPL5");

  sqlContext.sql("create table batsmen_cluster(player string,cluster_number int) row format delimited fields terminated by ','");
  sqlContext.sql("create table bowler_cluster(player string,cluster_number int) row format delimited fields terminated by ','");

  sqlContext.sql("LOAD DATA LOCAL INPATH './output/batsmen' OVERWRITE INTO TABLE batsmen_cluster");
  sqlContext.sql("LOAD DATA LOCAL INPATH './output/bowler' OVERWRITE INTO TABLE bowler_cluster");

  sc.stop()
