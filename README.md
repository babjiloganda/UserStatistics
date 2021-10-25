# UserStatistics
Spark Application to compute user app load metrics

Steps to Application:

The application need 2 arguments to run Parse/Statistics Mode.

1. Parse Mode:

List of Arguments:
    1. "Parse Mode"
    2. "local" or "yarn"(running in cluster)

2. Statistics Mode:

List of Arguments:
    1. "Statistics Mode"
    2. "local" or "yarn"(running in cluster)
    
    
config.properties:

SOURCE_PATH = "source file path/directory"
REGISTERED_PATH = "source/target path for Registered Events"
APP_LOADED_PATH = "source/target path for App Loaded Events"


#Spark Submit to run in Cluster

spark-submit 
--class com.miro.user.stats.UserStatistics 
--deploy-mode cluster  
--name UserStatistics  
--driver-memory 2g  
--executor-memory 2g  
--executor-cores 2  
--num-executors 2  
--files /home/user/config.properties#config.properties 
/home/user/user-statistics-1.0-SNAPSHOT.jar 
'Parse Mode' 
'yarn'