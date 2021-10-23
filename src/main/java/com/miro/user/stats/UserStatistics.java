package com.miro.user.stats;

import com.miro.user.stats.exception.CustomException;
import com.miro.user.stats.util.ConfigReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static com.miro.user.stats.constants.Constants.PARSE_MODE;
import static com.miro.user.stats.constants.Constants.STATISTICS_MODE;
import static com.miro.user.stats.impl.DataParser.parseData;
import static com.miro.user.stats.impl.MetricsBuilder.getUserStatistics;
import static com.miro.user.stats.util.Converters.convertTypes;

public class UserStatistics {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserStatistics.class.getName());

    public static LongAccumulator registeredCountAccumulator = new LongAccumulator();
    public static LongAccumulator appLoadedCountAccumulator = new LongAccumulator();

    public static void main(String[] args) throws CustomException {

        String executionMode;
        String master;

        try {

            if (args.length == 2) {
                executionMode = args[0];
                master = args[1];
            } else {
                throw new CustomException("Invalid number of Arguments; Required Arguments 'Execution Mode' , 'Spark Master' ");
            }

            SparkSession sparkSession = SparkSession
                    .builder()
                    .master(master)
                    .getOrCreate();

            // Register UDFs
            convertTypes(sparkSession);

            Properties properties = ConfigReader.readConfig();

            if (executionMode.equals(PARSE_MODE)) {

                parseData(sparkSession, properties);

            } else if (executionMode.equals(STATISTICS_MODE)) {

                long turnAroundPercentage = getUserStatistics(sparkSession, properties);
                System.out.println("Metric: " + turnAroundPercentage + "%");
                LOGGER.info("Metric: " + turnAroundPercentage + "%");

            } else {
                throw new CustomException("Invalid Run Mode : '" + args[0] + "' Kindly use one of these two modes {Parse Mode/Statistics Mode}");
            }

        } catch (CustomException | IOException customException) {
            LOGGER.error(" User Statistics Exception ");
            customException.printStackTrace();
        }

    }

}
