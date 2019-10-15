## Sports Result Prediction
Predicting the winner of IPL(Indian Premier League) matches using the data of players from previous matches using Predictive analysis.

- The project uses the players' statistics, data from previous matches and team performance to predicts the winner of a new IPL match given the members in the team.
- The outcome of each ball is determined using a decision-based algorithm to simulate the entire match. 
- The outcome of next year matches is predicted with an accuracy of 86%.

#### Technologies
- Python 
- Spark
- Hive
- HBase
- Mlib

#### Design
- The first stage of building the predictive system involved collecting Player vs Player data, which is obtained from ESPN Cricinfo database. Data scraping is done to collect the player vs player data using Python program `Beautiful Soup` and stored in a CSV file. 
- The data obtained only contains statistics on each players' encounter with another player. In order to solve the problem of a batsman and bowler who haven't encountered each other the players have to be grouped together so a new player falls into one of these groups.
- This is achieved using the `K-means algorithm` to cluster similar players in one category from player stats such as strike rate and the average for batsmen and economy and strike rate for bowlers. With multiple trials, the players were grouped into 8 clusters which is the optimum number for clusters for the data. After clustering, we load the data back into CSV files and write it into `Hive` making querying and analyzing easy. 
- Cumulative probabilities of the groups of batsmen and the bowlers scoring a 0, 1, 2, 3, 4 and 6 against another group is calculated. The probabilities data is loaded into `HBase` for quick random access. Using these probabilities a new match is simulated by taking the probability of a batsman scoring runs against a bowler. The batsmen get out when probability decreases below a threshold. 

*Multiple technologies are used to learn the usage of them for Big Data course*

#### Steps to Run code
- Move the Player vs Player data into Spark
- Run `python compute_cluster.py` to group similar players into clusters and store in Hive.
- Run `python cluster_vs_cluster.py` to compute cluster vs cluster data with probabilities and store in HBase.
- Store new team members in Team1.txt and Team2.txt
- Run `python IPL.py` to simulate the match and predict the winner.

#### Snapshot of the prediction - 1st Match in IPL 2016. Team 1 - MI vs Team 2 - RPS. Winner Team 2

![](https://github.com/shreyas-gopalakrishna/Sports-Result-Prediction/blob/master/IPL%20Prediction/IPL.png)
