package org.soltys.spark.demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.soltys.spark.demo.service.WordCounterRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * This class contains scenario to run the demo
 *
 * @author Mykhailo Soltys
 */
@Component
public class SparkDemoRunner implements CommandLineRunner {

    @Autowired private WordCounterRDD wordCounterRDD;
    @Autowired private JavaSparkContext javaSparkContext;


    @Override
    public void run(String... strings) throws Exception {
        runRddDemo();
    }

    private void runRddDemo() {
        JavaRDD<String> rdd = javaSparkContext.textFile("src/main/resources/big.txt");
        System.out.println("RDD | Top 10 words are " + wordCounterRDD.topWords(rdd, 10));
    }
}