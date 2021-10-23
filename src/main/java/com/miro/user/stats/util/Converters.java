package com.miro.user.stats.util;

import com.miro.user.stats.UserStatistics;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.miro.user.stats.constants.Constants.DATE_FORMAT;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class Converters {

    private static final Logger LOGGER = LoggerFactory.getLogger(Converters.class.getName());

    /**
     * Register Spark UDFs
     *
     * @param sparkSession Spark Session for Dataframe operations
     */
    public static void convertTypes(SparkSession sparkSession) {

        // Format input TimeStamp
        UDF1<String, String> getTimeStamp = new UDF1<String, String>() {

            public String call(String timeStamp) throws Exception {
                return getTimestamp(timeStamp, DATE_FORMAT);
            }
        };

        sparkSession.udf().register("toTimeStamp", getTimeStamp, StringType);

        // Compute AppLoad Metrics Flag
        UDF4<Integer, Integer, Integer, Integer, Integer> getAppLoadedStatus =
                new UDF4<Integer, Integer, Integer, Integer, Integer>() {

                    public Integer call(Integer yearRegistered, Integer weekRegistered,
                                        Integer yearAppLoaded, Integer weekAppLoaded) {
                        return getAppLoadedStatus(yearRegistered, weekRegistered, yearAppLoaded, weekAppLoaded);
                    }
                };

        sparkSession.udf().register("getAppLoadedStatus", getAppLoadedStatus, IntegerType);

    }

    /**
     * Method to format ISO timestamp to timestamp
     *
     * @param inputDate input Date in ISO format
     * @param inputFormat input Date format
     * @return TimeStamp formatted
     * @throws ParseException throws date parse exception
     */
    static String getTimestamp(String inputDate, String inputFormat) throws ParseException {

        SimpleDateFormat format = new SimpleDateFormat(inputFormat);
        String timeStamp = "";

        if (inputDate != null) {
            Date date = format.parse(inputDate);
            Timestamp timestamp = new Timestamp(date.getTime());
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            timeStamp = simpleDateFormat.format(timestamp);
        }

        return timeStamp;

    }

    /**
     * Method to compute AppLoad Metrics Flag
     *
     * @param yearRegistered Registered Event Year
     * @param weekRegistered Registered Event Week
     * @param yearAppLoaded Apploaded Event Year
     * @param weekAppLoaded Apploaded Event Week
     * @return '1' for App Loaded in immediate week
     */
    static int getAppLoadedStatus(int yearRegistered, int weekRegistered,
                                  int yearAppLoaded, int weekAppLoaded) {

        int appLoadedIndicator = 0;

        if (yearAppLoaded == yearRegistered && weekAppLoaded - weekRegistered == 1) {
                appLoadedIndicator = 1;
        } else if (yearAppLoaded - yearRegistered == 1 && weekAppLoaded - weekRegistered == -52 || weekAppLoaded - weekRegistered == -51) {
                appLoadedIndicator = 1;
        }

        return appLoadedIndicator;

    }

    /**
     * Method to add App Load and Registered Count to Global Accumulator
     *
     * @param row Dataframe row
     * @return Dataframe row after Accumulator count
     */
    public static Row addAppLoadCount(Row row) {

        LOGGER.info("Convertors : App Load Count ");

        UserStatistics.registeredCountAccumulator.add(1);
        if (row.getInt(row.fieldIndex("app_loaded_status")) == 1) {
            UserStatistics.appLoadedCountAccumulator.add(1);
        }

        return row;

    }

}
