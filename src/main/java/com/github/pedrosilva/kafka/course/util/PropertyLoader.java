package com.github.pedrosilva.kafka.course.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyLoader {

    public static Properties getConfigurations() throws IOException
    {
        InputStream input = PropertyLoader.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        properties.load(input);
        return properties;
    }
}
