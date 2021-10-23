package com.miro.user.stats.impl;

import com.miro.user.stats.util.ConfigReader;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static com.miro.user.stats.util.Converters.convertTypes;

public class MetricsBuilderTest {

    SparkSession sparkSession;
    Properties properties;

    @Before
    public void setUp() throws IOException {

        sparkSession = SparkSession
                .builder()
                .appName("User Statistics Test")
                .master("local")
                .getOrCreate();

        properties = ConfigReader.readConfig();

        // Register UDFs
        convertTypes(sparkSession);
    }


    @Test
    public void testGetUserStatistics() {

        long metricPercentage = MetricsBuilder.getUserStatistics(sparkSession, properties);

        Assert.assertEquals(37, metricPercentage);
    }

}