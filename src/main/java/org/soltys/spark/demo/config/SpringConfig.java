package org.soltys.spark.demo.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

/**
 * Bean declarations
 *
 * @author Mykhailo Soltys
 */
@Configuration
@ComponentScan("org.soltys.spark")
@PropertySource("classpath:common.properties")
public class SpringConfig {

    @Bean
    SparkConf sparkConf(){
        Logger.getLogger("org.apache").setLevel(Level.OFF);
        // local cluster with 3 nodes
        return new SparkConf().setAppName("spark-demo").setMaster("local[3]");
    }

    @Bean
    JavaSparkContext javaSparkContext(SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }

    @Bean
    SparkSession sparkSession(SparkConf sparkConf){
        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer configurer(){
        return new PropertySourcesPlaceholderConfigurer ();
    }


}
