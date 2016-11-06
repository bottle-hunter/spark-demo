package org.soltys.spark.demo.service;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.soltys.spark.demo.config.SpringConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = SpringConfig.class)
public class SparkDemoTest {

    @Autowired
    JavaSparkContext sparkContext;

    @Autowired
    WordCounterRDD wordCounterRDD;

    @Test
    public void testInMemoryTopWords() {
        JavaRDD<String> rdd = sparkContext.parallelize(Arrays.asList("python java scala java groovy scala java php java"));
        List<String> top1 = wordCounterRDD.topWords(rdd, 1);
        Assert.assertEquals("java", top1.get(0));
    }

}