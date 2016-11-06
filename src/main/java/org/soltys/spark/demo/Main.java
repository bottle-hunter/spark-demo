package org.soltys.spark.demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.soltys.spark.demo.config.SpringConfig;
import org.soltys.spark.demo.service.FilmsProcessor;
import org.soltys.spark.demo.service.WordCounter;
import org.soltys.spark.demo.service.WordCounterDataset;
import org.soltys.spark.demo.service.WordCounterRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import java.util.List;


/**
 * This class is entry point
 *
 * @author Mykhailo Soltys
 */
@Configuration
@Import(SpringConfig.class)
public class Main {

    public static void main(String... ars) {
        SpringApplication.run(Main.class);
    }
}


/**
 * Contains demo scenarios
 */
@Component
class SparkDemoRunner implements CommandLineRunner {

    private static final String PATH_TO_RESOURCES = "src/main/resources/";

    @Autowired private JavaSparkContext javaSparkContext;
    @Autowired private SparkSession sparkSession;
    @Autowired private WordCounterRDD wordCounterRDD;
    @Autowired private WordCounterDataset wordCounterDataset;
    @Autowired private FilmsProcessor filmsProcessor;


    @Override
    public void run(String... strings) throws Exception {
        rddWordCounterDemo(PATH_TO_RESOURCES + "big.txt");
        datasetWordCounterDemo(PATH_TO_RESOURCES + "big.txt");
        datasetFromJsonDemo(PATH_TO_RESOURCES + "films.json");
    }


    private void rddWordCounterDemo(String filePath){
        System.out.println("========================== RDD ==========================");
        JavaRDD<String> rddOfTextLines = javaSparkContext.textFile(filePath);
        List<String> top10 = wordCounterRDD.topWords(rddOfTextLines, 10);
        System.out.println("Top 10 words from input text : " + top10);
    }

    private void datasetWordCounterDemo(String filePath){
        System.out.println("============ Dataset without predefined schema ===========");
        JavaRDD<String> rddOfTextLines = javaSparkContext.textFile(filePath);
        JavaRDD<String> rddOfwords = rddOfTextLines.flatMap(WordCounter::splitWords);

        Dataset<Row> RowDataset = sparkSession.createDataFrame(
                rddOfwords.map(RowFactory::create),                      // maps to JavaRDD<Row>
                DataTypes.createStructType(new StructField[]{            // programmatically define schema
                        DataTypes.createStructField("words", DataTypes.StringType, true)
                }));

        List<String> top15 = wordCounterDataset.topWords(RowDataset, 15);
        System.out.println("Top 15 words from input text : " + top15);
    }

    private void datasetFromJsonDemo(String filePath) {
        System.out.println("===================== Dataset from JSON ====================");
        Dataset<Row> filmsDataset = sparkSession.read().json(filePath);
        List<String> films = filmsProcessor.getFilmsWithMostPopularActor(filmsDataset);
        System.out.println("Films with most popular actor : " + films);
    }

}



