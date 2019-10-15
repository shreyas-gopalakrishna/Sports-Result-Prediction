from pyspark import SparkContext, SparkConf
from hbase_operations import HBase
from collections import defaultdict
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
import pickle

conf = SparkConf().setAppName('Hive')
sc = SparkContext(conf=conf)

sqlContext = HiveContext(sc)

sqlContext.sql("use IPL5");

batsmen_cluster = sqlContext.sql("Select * from batsmen_cluster");
bowler_cluster = sqlContext.sql("Select * from bowler_cluster");

#print(batsmen_cluster.show())
#print(bowler_cluster.show())

batsmen_cluster = batsmen_cluster.rdd;
batsmen_to_cluster = batsmen_cluster.collectAsMap()
print(batsmen_to_cluster)

bowler_cluster = bowler_cluster.rdd;
bowler_to_cluster = bowler_cluster.collectAsMap()
print(bowler_to_cluster)
'''
#loading data
batsmen_cluster = pickle.load(open("batsmen.p","rb"))
#loading bowl data
bowler_cluster = pickle.load(open("bowler.p","rb"))
'''
player_vs_player = pickle.load(open("player_vs_player.p","rb"))


cluster_batsmen = defaultdict(list)
cluster_bowler = defaultdict(list) #[0,0,0,0,0,0,0,0]


for k,v in batsmen_to_cluster.items() :
    cluster_batsmen[int(v)].append(k)
        
for k,v in bowler_to_cluster.items() :
    cluster_bowler[int(v)].append(k)

#Dict - k,v v - dict - List
d = {}
no_of_clusters  = 8

cluster_vs_cluster = [[0 for x in range(no_of_clusters)] for y in range(no_of_clusters)]
#print(cluster_vs_cluster)
for i in range(0,no_of_clusters):
    for j in range(0,no_of_clusters):
        L = [0,0,0,0,0,0,0,0,0,0,0]
        cluster_vs_cluster[i][j] = L

#player_vs_player 

for k,v in player_vs_player.items():
    
    bat = k
    
    bat_cluster_no = int(batsmen_to_cluster[k])
    
    for i in range(0,len(v)):
        d = v[i]
        for k1,v1 in d.items():
            bowl = k1
            
            bowl_cluster_no = int(bowler_to_cluster[str(k1)])
            
            cluster_vs_cluster[bat_cluster_no][bowl_cluster_no] = [x + y for x,y in zip(cluster_vs_cluster[bat_cluster_no][bowl_cluster_no],v1)]
                
for i in range(len(cluster_vs_cluster)):
    print(cluster_vs_cluster[i])
    arr = ''
    for j in cluster_vs_cluster[i]:
        print(j)
        inner = ":".join(map(str, j))
        arr += inner +";"
        
    d[str(i)] = arr;
    #print(d[k])
    

pickle.dump(cluster_vs_cluster,open("cluster_vs_cluster.p","wb"),protocol=2)
print(d)
 
 
hbase = HBase()
hbase.delete_all_tables()
hbase.create_cluster_table() 
hbase.add_clusters(d)
#l = hbase.get_cluster_stats(1,2)
#print(l)
