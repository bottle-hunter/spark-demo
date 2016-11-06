# Spark demo application
Spark 2.0.1 + Spring Boot application + Maven

Demonstrates how to start work with Spark and 2 key features of last:
* RDD
* Datasets (DataFrames)

# What's inside?
SpringConfig - Spring configuration class, where Spark infrastructure beans are defined.
SparkDemoRunner - Contains demo scenarios to run.
WordCounterRDD - class to demonstrate work with RDD (JavaRDD).
WordCounterDataset - class to demonstrate work with Dataset without predefined schema.
FilmsProcessor - class demonstrates work with Dataset with predefined structure (JSON in this case).
PropertiesHolder - class to demonstrate how to share configs on Spark nodes without use of broadcast.

# How to run?
To run demo in standalone mode you don't need any additional installations. Just run main() method of Main class.

_NOTE: to see the spark logs change the logging level in SparkConf bean declaration in SpringConfig class. For ex:
```
    @Bean SparkConf sparkConf(){
        Logger.getLogger("org.apache").setLevel(Level.INFO);
        ...
    }
```