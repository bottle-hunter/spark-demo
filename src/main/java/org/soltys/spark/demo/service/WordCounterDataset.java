package org.soltys.spark.demo.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

/**
 * Class to demonstrate work with {@link Dataset} without predefined schema
 *
 * @author Mykhailo Soltys
 */
@Component
public class WordCounterDataset implements WordCounter {

    @Autowired private PropertiesHolder properties;

    public List<String> topWords(Dataset<Row> words, int count) {
        Dataset<Row> sort = words.withColumn("words", lower(column("words")))
                .filter(not(column("words").isin(properties.getStopWords().toArray())))     // not in stop words
                .groupBy(column("words")).agg(count("words").as("count"))                   // aggregate
                .sort(column("count").desc());                                               // sort desc

        //sort.show();

        // classic java streams
        return sort.takeAsList(count).stream()
                .map(row -> row.getString(0))
                .collect(Collectors.toList());
    }
}
