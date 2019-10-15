import happybase
import pickle
import random

class HBase:
    def __init__(self, host_='localhost', port_=9090, timeout_=10000):
        self.conn = happybase.Connection(host=host_, port=port_, timeout=timeout_)
        
    def terminate(self):
        self.conn.close()
        
    def delete_all_tables(self):
        print("Following tables will be deleted (old tables) :",self.conn.tables())
        for table_name in self.conn.tables():
            self.conn.delete_table(table_name, disable=True)
        
    def create_bowler_table(self):
        self.conn.create_table("Bowler",{"Bowler:GroupNo": dict()})
        
    def create_batsman_table(self):
        self.conn.create_table("Batsman",{"Batsman:GroupNo": dict()})
        
    def create_cluster_table(self):
        self.conn.create_table("Cluster",{"Cluster:GroupNo": dict()})
        
    def get_bowler_group(self, name):
        table = self.conn.table("Bowler")
        row = table.row(name.encode("utf-8"))
        return int(row["Bowler:GroupNo".encode('utf-8')])
        
    def get_batsman_group(self, name):
        table = self.conn.table("Batsman")
        row = table.row(name.encode("utf-8"))
        return int(row["Batsman:GroupNo".encode('utf-8')])
        
    def get_cluster_stats(self, Gbat, Gbowl):
        name = str(Gbat)
        table = self.conn.table("Cluster")
        row = table.row(name.encode("utf-8"))
        tmp = row["Cluster:GroupNo".encode('utf-8')]
        tmp = tmp.decode('utf-8').split(";")[Gbowl]
        return [int(x) for x in tmp.split(":")]
 
    def add_bowlers(self, maps):
        table = self.conn.table("Bowler")
        for key,value in maps.items():
            table.put(key, {"Bowler:GroupNo": str(value)}) 
        
    def add_batsmans(self, maps):
        table = self.conn.table("Batsman")
        for key,value in maps.items():
            table.put(key, {"Batsman:GroupNo": str(value)})
            
    def add_clusters(self, maps):
        table = self.conn.table("Cluster")
        for key,value in maps.items():
            table.put(key, {"Cluster:GroupNo": str(value)})
