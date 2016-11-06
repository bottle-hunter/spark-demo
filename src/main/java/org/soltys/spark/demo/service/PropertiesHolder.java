package org.soltys.spark.demo.service;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Common properties container, intended to be used by services to obtain properties. Implements {@link Serializable},
 * as it will be transferred by Spark to cluster nodes, where services are executing.
 *
 * <b>Note</b> All beans which contains this class as a member, should also implements {@link Serializable}.
 * Such approach  does'nt require to do Spark broadcast.
 *
 * @author Mykhailo Soltys
 */
@Component
public class PropertiesHolder implements Serializable {

    private List<String> articles;  // list of articles
    private List<String> stopWords; // list of words skipped by search engines

    @Value("${articles}")
    public void setArticles(String[] blacklist) {
        this.articles = Arrays.asList(blacklist);
    }

    @Value("${stop.words}")
    public void setStopWords(String[] stopWords) {
        this.stopWords = Arrays.asList(stopWords);
    }


    public List<String> getStopWords() {
        return stopWords;
    }


    public List<String> getArticles() {
        return articles;
    }

}
