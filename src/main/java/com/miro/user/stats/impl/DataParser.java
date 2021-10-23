package com.miro.user.stats.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.miro.user.stats.constants.Constants.*;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class DataParser {

    private DataParser(){

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DataParser.class.getName());

    /**
     * Method to Parse input json files and store it in Parquet format
     *
     * @param sparkSession Spark Session for Dataframe operations
     */
    public static void parseData(SparkSession sparkSession, Properties properties) {

        LOGGER.info("UserStatistics : Parse Mode Execution ");

        Dataset<Row> inputDF = sparkSession
                .read()
                .json(properties.getProperty(SOURCE_PATH));

        inputDF = inputDF
                .withColumn("time",
                        callUDF(
                                "toTimeStamp",
                                col("timestamp")
                        ).cast(TimestampType)
                ).drop("timestamp");

        Dataset<Row> registeredDf = inputDF
                .filter(col("event").equalTo(REGISTERED))
                .withColumnRenamed("device_type", "channel");

        Dataset<Row> appLoadedDf = inputDF
                .filter(col("event").equalTo(APP_LOADED));

        registeredDf
                .write()
                .option("header", true)
                .save(properties.getProperty(REGISTERED_PATH));

        appLoadedDf
                .write()
                .option("header", true)
                .save(properties.getProperty(APP_LOADED_PATH));

        LOGGER.info(" UserStatistics : parseData :: Parquet Files Write Completed ");

    }
}
