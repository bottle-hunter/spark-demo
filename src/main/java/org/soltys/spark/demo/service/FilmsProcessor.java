package org.soltys.spark.demo.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Class demonstrates work with Dataset with predefined structure (JSON in this case).
 *
 * @author Mykhailo Soltys
 */
@Component
public class FilmsProcessor {

    public List<String> getFilmsWithMostPopularActor(Dataset<Row> films) {
        System.out.println("Input Dataset of films:");
        films.show(false);

        // new Dataset with listed all actor name occurrences
        Dataset<Row> actors = films.select(functions.explode(functions.column("actors")).as("actor"));

        // new Dataset with "actor" and "count" columns
        Dataset<Row> actorsByPopularity = actors.groupBy("actor")
                .agg(functions.count("actor").as("count"))
                .orderBy(functions.column("count").desc());
        System.out.println("Top 10 Actors by occurrences in films:");
        actorsByPopularity.show(10);

        // get from top of 'table'
        String mostPopularActor = actorsByPopularity.first().getString(0);

        // Find films with most popular actor
        Dataset<Row> filmsWithMostPopularActor =
                films.where(functions.array_contains(functions.column("actors"), mostPopularActor))
                .select("name");

        // to List
        return filmsWithMostPopularActor.collectAsList()
                .stream().map(row -> row.getString(0))
                .collect(Collectors.toList());
    }
}
