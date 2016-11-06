package org.soltys.spark.demo.service;

import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.List;

/**
 * Class to demonstrate work with RDD ({@link JavaRDD})
 *
 * @author Mykhailo Soltys
 */
@Component
public class WordCounterRDD implements WordCounter {

    @Autowired private PropertiesHolder properties;

    public List<String> topWords(JavaRDD<String> words, int count) {
        return words.map(String::toLowerCase)                               // RDD of text lines
                .flatMap(WordCounter::splitWords)                           // RDD of words
                .filter(word -> !properties.getStopWords().contains(word))  // filter words
                .mapToPair(word -> new Tuple2<>(word, 1))                   // {"word" : 1}
                .reduceByKey((a, b) -> a + b)                               // {"word" : count}
                .mapToPair(Tuple2::swap)                                    // {count : "word"}
                .sortByKey(false)                                           // sort descend
                .map(Tuple2::_2)                                            // get only word
                .take(count);                                               // return list of words
    }
}
