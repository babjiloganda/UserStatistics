package com.miro.user.stats.impl;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.miro.user.stats.UserStatistics.appLoadedCountAccumulator;
import static com.miro.user.stats.UserStatistics.registeredCountAccumulator;
import static com.miro.user.stats.constants.Constants.APP_LOADED_PATH;
import static com.miro.user.stats.constants.Constants.REGISTERED_PATH;
import static com.miro.user.stats.util.Converters.addAppLoadCount;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class MetricsBuilder {

    private MetricsBuilder(){

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsBuilder.class.getName());

    /**
     * This Method will compute the TURN AROUND METRICS between Registered and App Loaded Users
     *
     * @param sparkSession Spark Session for Dataframe operations
     */
    public static long getUserStatistics(SparkSession sparkSession, Properties properties) {

        LOGGER.info(" UserStatistics : Statistics Mode Execution ");

        Dataset<Row> registeredDF = getRegisteredDf(sparkSession, properties);

        Dataset<Row> appLoadedDF = getAppLoadedDf(sparkSession, properties);

        Dataset<Row> joinedDf = registeredDF.join(appLoadedDF,
                registeredDF.col("initiator_id_registered")
                        .equalTo(appLoadedDF.col("initiator_id_app_loaded")), "left_outer");

        // Added Custom Stage to persist the joined Data to avoid the re-computation of Join.
        joinedDf.persist();

        joinedDf.foreachPartition(iter -> {
            iter.hasNext();
        });

        joinedDf = joinedDf
                .orderBy("time_registered", "time_app_loaded")
                .dropDuplicates("initiator_id_registered")
                .withColumn("app_loaded_status",
                        callUDF("getAppLoadedStatus",
                                col("year_registered"),
                                col("week_registered"),
                                col("year_app_loaded"),
                                col("week_app_loaded")
                        )
                );

        ExpressionEncoder<Row> encoder = RowEncoder.apply(joinedDf.schema());

        joinedDf.map((MapFunction<Row, Row>) x -> addAppLoadCount(x), encoder).count();

        long turnAroundPercentage = (appLoadedCountAccumulator.value() * 100 / registeredCountAccumulator.value());

        LOGGER.info(" UserStatistics : getUserStatistics :: User Stats Loaded");

        return turnAroundPercentage;

    }

    /**
     * Method to load Registered Dataframe from Parquet source
     *
     * @param sparkSession Spark Session for Dataframe operations
     * @param properties Properties holds file paths
     * @return Dataframe of Registered Events
     */
    private static Dataset<Row> getRegisteredDf(SparkSession sparkSession, Properties properties) {

        Dataset<Row> registeredDF = sparkSession
                .read()
                .parquet(properties.getProperty(REGISTERED_PATH));

        registeredDF = registeredDF
                .withColumn("year_registered", functions.year(col("time")))
                .withColumn("week_registered", functions.weekofyear(col("time")))
                .withColumnRenamed("time", "time_registered")
                .withColumnRenamed("initiator_id", "initiator_id_registered")
                .withColumnRenamed("event", "event_registered")
                .drop("channel");

        return registeredDF;
    }

    /**
     * Method to load AppLoaded Dataframe from Parquet source
     *
     * @param sparkSession Spark Session for Dataframe operations
     * @param properties Properties holds file paths
     * @return Dataframe of AppLoaded Events
     */
    private static Dataset<Row> getAppLoadedDf(SparkSession sparkSession, Properties properties) {

        Dataset<Row> appLoadedDF = sparkSession
                .read()
                .parquet(properties.getProperty(APP_LOADED_PATH));

        appLoadedDF = appLoadedDF
                .withColumn("year_app_loaded", functions.year(col("time")))
                .withColumn("week_app_loaded", functions.weekofyear(col("time")))
                .withColumnRenamed("time", "time_app_loaded")
                .withColumnRenamed("initiator_id", "initiator_id_app_loaded")
                .withColumnRenamed("event", "event_app_loaded")
                .drop("device_type");

        return appLoadedDF;
    }
}
