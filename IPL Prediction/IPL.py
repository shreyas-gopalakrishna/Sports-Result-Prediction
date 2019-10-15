#from pyspark import SparkContext, SparkConf
#from hbase_operations import HBase
#from pyspark.sql import HiveContext
#from pyspark import SparkContext, SparkConf
from collections import defaultdict
import pickle
import random


def per_ball_outcome(cluster_cell):
    total_no_of_balls = cluster_cell[10] - cluster_cell[8]
    
    cluster_cell_probability = []
    if(total_no_of_balls == 0):
        cluster_cell_probability = [0.125,0.125,0.125,0.125,0.125,0.125,0.125,0.125]
    else:
        for i in range(0,8):
            cluster_cell_probability.append(cluster_cell[i]/total_no_of_balls)
    
    random_number = random.random()
    
    
    cumulative_cluster_cell_probability = [sum(cluster_cell_probability[0:i+1]) for i in range(0,8)]
    
    #if( random_number <= cumulative_cluster_cell_probability[0] ):
    #    return 0
    if(random_number > cumulative_cluster_cell_probability[0]  and random_number <= cumulative_cluster_cell_probability[1]):
        return 1
    elif(random_number > cumulative_cluster_cell_probability[1]  and random_number <= cumulative_cluster_cell_probability[2]):
        return 2
    elif(random_number > cumulative_cluster_cell_probability[2]  and random_number <= cumulative_cluster_cell_probability[3]):
        return 3
    elif(random_number > cumulative_cluster_cell_probability[3]  and random_number <= cumulative_cluster_cell_probability[4]):
        return 4
    elif(random_number > cumulative_cluster_cell_probability[4]  and random_number <= cumulative_cluster_cell_probability[5]):
        return 5
    elif(random_number > cumulative_cluster_cell_probability[5]  and random_number <= cumulative_cluster_cell_probability[6]):
        return 6
    elif(random_number > cumulative_cluster_cell_probability[6]  and random_number <= cumulative_cluster_cell_probability[7]):
        return 7
    else:
        return 0
    
    '''temp = cluster_cell_probability[0]
    cumulative_cluster_cell_probability = []
    cumulative_cluster_cell_probability.append(temp)
    for i in range(1,8):
        cumulative_cluster_cell_probability.append(cumulative_cluster_cell_probability[i-1]+cluster_cell_probability)'''
    
def swap_batsmen():
    global onStrike
    global nonStrike
    global survival_prob_onStrike
    global survival_prob_nonStrike
    global onStrikeScore
    global nonStrikeScore
        
    temp = onStrike
    onStrike = nonStrike
    nonStrike = temp
    
    temp1 = survival_prob_onStrike
    survival_prob_onStrike = survival_prob_nonStrike
    survival_prob_nonStrike = temp1
    
    temp2 = onStrikeScore
    onStrikeScore = nonStrikeScore
    nonStrikeScore = temp2



#loading data
batsmen_cluster = pickle.load(open("batsmen.p","rb"))

#loading bowl data
bowler_cluster = pickle.load(open("bowler.p","rb"))

cluster_batsmen = defaultdict(list)
cluster_bowler = defaultdict(list) 

    
# batsmen_cluster - has key as batsmen, value as cluster number
# cluster_batsmen - has key as cluster, value as list of batsmen


for k,v in batsmen_cluster.items():
    cluster_batsmen[int(v)].append(k)
        
for k,v in bowler_cluster.items():
    cluster_bowler[int(v)].append(k)

#Dict - k,v v - dict - List

#hbase = HBase()

cluster_vs_cluster = pickle.load(open("cluster_vs_cluster.p","rb"))


print(" Welcome to IPL ")
print("")

toss = int(input("Enter who wins the toss \n1 - Team 1 \n2 - Team 2\n\n"))


while(not(toss == 1 or toss == 2) ):
    print("Wrong Option! Enter again \n")
    toss = int(input("Enter who wins the toss \n1 - Team 1 \n2 - Team 2\n\n"))

# Team who wins the toss is team 1, just interchanging the file reads
print("")
if(toss == 1):
    team1_order = open("Team1.txt","r")
    team2_order = open("Team2.txt","r")
else:
    team1_order = open("Team2.txt","r")
    team2_order = open("Team1.txt","r")

    
team1_batting_order_temp = team1_order.read().split("\n")

team1_batting_order = team1_batting_order_temp[1:12]
team1_bowling_order = team1_batting_order_temp[13:33]

#print((team1_batting_order))
#print((team1_bowling_order))

team2_batting_order_temp = team2_order.read().split("\n")

team2_batting_order = team2_batting_order_temp[1:12]
team2_bowling_order = team2_batting_order_temp[13:33]

#print(team2_batting_order)
#print(team2_bowling_order)

print("---------------------------------------------------------\n")

innings = 1
overs = 0
balls = 0

innings_1_total_runs = 0
innings_1_total_wickets = 0

innings_2_total_runs = 0
innings_2_total_wickets = 0

onStrike = team1_batting_order[0]
nonStrike = team1_batting_order[1]
nextBatsmen = 2

hightestScore = 0
hightestScorer = ""

onStrikeScore = 0
nonStrikeScore = 0

bowler = team2_bowling_order[0]
nextBowler = 1

survival_prob_onStrike = 1 
survival_prob_nonStrike = 1 

#while(not(overs == 20 and balls == 6)):
while(overs<=20):
    
    onStrike_cluster = int(batsmen_cluster[onStrike])
    bowler_bowling_cluster = int(bowler_cluster[bowler])
    
    #print(onStrike_cluster,bowler_bowling_cluster)
    #print(nextBowler)
    cluster_cell = cluster_vs_cluster[onStrike_cluster][bowler_bowling_cluster]
    
    if(cluster_cell[10] == 0):
        survival_prob_onStrike = survival_prob_onStrike*0.95
    else:
        survival_prob_onStrike = survival_prob_onStrike * (1 - ((cluster_cell[8]) / (cluster_cell[10])))
    
    if(survival_prob_onStrike < 0.50):
        #playerout
        innings_1_total_wickets += 1
        print(onStrike + " got out\n")
        print(" Score - " , innings_1_total_runs , "/",innings_1_total_wickets ,"\n")
        
        if(onStrikeScore > hightestScore):
            hightestScore = onStrikeScore
            hightestScorer = onStrike
        
        if(innings_1_total_wickets >= 10):
            pass
        else:
            onStrike = team1_batting_order[nextBatsmen]
            onStrikeScore = 0
            survival_prob_onStrike = 1
            nextBatsmen += 1
    else:
        runs = per_ball_outcome(cluster_cell)
        onStrikeScore += 1
        innings_1_total_runs += runs
        
        if(runs % 2 != 0):
            swap_batsmen()
    
    balls += 1        
    
    if(balls == 6 and innings_1_total_wickets < 10):
        swap_batsmen()
        overs += 1
        balls = 0
        print("Overs ",overs," Score ",innings_1_total_runs, "/",innings_1_total_wickets)
        if(overs < 20):
            bowler = team2_bowling_order[nextBowler]
            nextBowler += 1
        
    if(innings_1_total_wickets == 10):
        break
        
    if(overs == 20 and balls == 6):
        break
        
    #print("overs ", overs ," balls ", balls ," \n")       
    
print(" \n------------------End of innings 1------------------\n")
print("Innings 1 Score - " , innings_1_total_runs , "/",innings_1_total_wickets ,"\n")
print("Highest Scorer ", hightestScorer," - ",hightestScore)
print("--------------------------------------------------------")    

            
#end of innings 1 code and start of innings 2 code

innings = 2
overs = 0
balls = 0


onStrike = team2_batting_order[0]
nonStrike = team2_batting_order[1]
nextBatsmen = 2

hightestScore1 = 0
hightestScorer1 = ""

onStrikeScore1 = 0
nonStrikeScore = 0

bowler = team1_bowling_order[0]
nextBowler = 1

survival_prob_onStrike = 1 
survival_prob_nonStrike = 1 

#while(not(overs == 20 and balls == 6)):
while(overs<=20):   
    onStrike_cluster = int(batsmen_cluster[onStrike])
    bowler_bowling_cluster = int(bowler_cluster[bowler])
    
    cluster_cell = cluster_vs_cluster[onStrike_cluster][bowler_bowling_cluster]
    
    if(cluster_cell[10] == 0):
        survival_prob_onStrike = survival_prob_onStrike * 0.95
    else:
        survival_prob_onStrike = survival_prob_onStrike * (1 - ((cluster_cell[8]) / (cluster_cell[10])))
    
    if(survival_prob_onStrike < 0.50):
        #playerout
        innings_2_total_wickets += 1
        print(onStrike + " got out\n")
        print(" Score - " , innings_2_total_runs , "/",innings_2_total_wickets ,"\n")
        
        if(onStrikeScore > hightestScore1):
            hightestScore1 = onStrikeScore
            hightestScorer1 = onStrike
            
        if(innings_2_total_wickets >= 10):
            pass
        else:
            onStrike = team2_batting_order[nextBatsmen]
            survival_prob_onStrike = 1
            onStrikeScore = 0
            nextBatsmen += 1
    else:
        runs = per_ball_outcome(cluster_cell)
        innings_2_total_runs += runs
        onStrikeScore += runs
        
        if(runs % 2 != 0):
            swap_batsmen()
    
    balls += 1        
    
    if(balls == 6 and innings_2_total_wickets < 10):
        swap_batsmen()
        overs += 1
        balls = 0
        print("Overs ",overs," Score ",innings_2_total_runs, "/",innings_2_total_wickets)
        if(overs < 20):
            bowler = team1_bowling_order[nextBowler]
            nextBowler += 1
    
    if(innings_2_total_runs > innings_1_total_runs):
        break
        
    if(innings_2_total_wickets == 10):
        break
        
    if(overs == 20 and balls == 6):
        break       
    
print(" \n------------------End of innings 2------------------\n")
print("Innings 2 Score - " , innings_2_total_runs , "/",innings_2_total_wickets ,"\n")
print("Highest Scorer ", hightestScorer1," - ",hightestScore1)
print("--------------------------------------------------------\n\n")    


print(" \n------------------Match Summary------------------\n")
print("Innings 1 Score - " , innings_1_total_runs , "/",innings_1_total_wickets ,"\n")
print("Innings 2 Score - " , innings_2_total_runs , "/",innings_2_total_wickets ,"\n")
if(innings_1_total_runs > innings_2_total_runs):
    print("Team 1 won by ",(innings_1_total_runs - innings_2_total_runs - 1)," runs ")
elif(innings_1_total_runs < innings_2_total_runs):
    print("Team 2 won by ",(10 - innings_2_total_wickets)," wickets")
else:
    print("What a dramatic finish! Its a tie :O ")
    
print()



