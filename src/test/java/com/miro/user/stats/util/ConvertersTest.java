package com.miro.user.stats.util;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

import static com.miro.user.stats.UserStatistics.appLoadedCountAccumulator;
import static com.miro.user.stats.UserStatistics.registeredCountAccumulator;
import static com.miro.user.stats.constants.Constants.DATE_FORMAT;
import static com.miro.user.stats.util.Converters.addAppLoadCount;

public class ConvertersTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConvertersTest.class.getName());

    SparkSession sparkSession = SparkSession
            .builder()
            .master("local")
            .getOrCreate();

    @Test
    public void testGetTimestamp() throws ParseException {

        LOGGER.info("ConvertersTest : Test GetTimestamp ");

        String timestamp = Converters.getTimestamp("2020-02-11T06:21:14.000Z", DATE_FORMAT);

        Assert.assertEquals("2020-11-02 06:21:14", timestamp);
        Assert.assertNotEquals("2020-01-02 06:21:15", timestamp);
    }

    @Test
    public void testGetAppLoadedStatus() {

        LOGGER.info("ConvertersTest : Test GetAppLoadedStatus ");

        int diffYearsSubsequentWeek = Converters.getAppLoadedStatus(2019, 52, 2020, 1);
        int sameYearSameWeek = Converters.getAppLoadedStatus(2020, 45, 2020, 45);
        int sameYearSubsequentWeek = Converters.getAppLoadedStatus(2020, 35, 2020, 36);

        Assert.assertEquals(1, diffYearsSubsequentWeek);
        Assert.assertEquals(0, sameYearSameWeek);
        Assert.assertEquals(1, sameYearSubsequentWeek);

    }

    @Test
    public void testAddAppLoadCount() {

        LOGGER.info("ConvertersTest : Test AddAppLoadCount ");

        Dataset<Row> inputDF = sparkSession
                .read()
                .option("inferSchema", true)
                .option("header", true)
                .csv("/Users/blogandarajaramk/ROMI/user-statistics/src/test/resources/appLoadCountTestFile.csv");

        inputDF.printSchema();

        ExpressionEncoder<Row> encoder = RowEncoder.apply(inputDF.schema());

        inputDF.map((MapFunction<Row, Row>) x -> addAppLoadCount(x), encoder).count();

        long appLoadedCount = appLoadedCountAccumulator.value();
        long registeredCount = registeredCountAccumulator.value();

        Assert.assertEquals(2, appLoadedCount);
        Assert.assertEquals(4, registeredCount);

    }
}