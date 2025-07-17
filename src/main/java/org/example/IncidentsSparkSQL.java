package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class IncidentsSparkSQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("IncidentsParService")
                .master("local[*]")
                .getOrCreate();

        // Lecture du fichier CSV
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/incidents.csv");

        df.printSchema();
        df.show();

        // 1. Nombre d’incidents par service
        System.out.println("==> Nombre d'incidents par service :");
        df.groupBy("service")
                .count()
                .orderBy(desc("count"))
                .show();

        // 2. Les deux années où il y a eu le plus d’incidents
        System.out.println("==> Les deux années avec le plus d'incidents :");
        Dataset<Row> incidentsParAnnee = df.withColumn("annee", year(col("date")))
                .groupBy("annee")
                .count()
                .orderBy(desc("count"))
                .limit(2);

        incidentsParAnnee.show();

        spark.stop();
    }
}
