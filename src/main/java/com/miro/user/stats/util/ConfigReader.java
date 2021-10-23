package com.miro.user.stats.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigReader.class.getName());

    public static Properties readConfig() throws IOException {

        Properties properties = new Properties();
        InputStream input = ConfigReader.class.getClassLoader().getResourceAsStream("config.properties");
        if (input == null) {
            LOGGER.info("Unable to find config.properties");
        }

        try {
            properties.load(input);
        } catch (IOException var4) {
            LOGGER.error("Unable to read config file: {}", var4.getMessage());
            throw new IOException();
        }

        return properties;
    }
}
