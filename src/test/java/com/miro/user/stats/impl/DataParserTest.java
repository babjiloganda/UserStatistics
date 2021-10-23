package com.miro.user.stats.impl;

import com.miro.user.stats.util.ConfigReader;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static com.miro.user.stats.util.Converters.convertTypes;

public class DataParserTest {

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
    public void testParseData() {

        properties.setProperty("REGISTERED_PATH", properties.getProperty("PARSE_REGISTERED_PATH"));
        properties.setProperty("APP_LOADED_PATH", properties.getProperty("PARSE_APP_LOADED_PATH"));

        DataParser.parseData(sparkSession, properties);

        long registeredCount = sparkSession
                .read()
                .option("header", true)
                .load(properties.getProperty("REGISTERED_PATH")).count();

        Assert.assertEquals(9,registeredCount);

        long appLoadedCount = sparkSession
                .read()
                .option("header", true)
                .load(properties.getProperty("APP_LOADED_PATH")).count();

        Assert.assertEquals(10,appLoadedCount);

    }
}