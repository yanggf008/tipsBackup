package com.adatafun.conf;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by yanggf on 2017/9/19.
 */
public class ApplicationProperty {
    private final Properties prop = new Properties();
    private static ApplicationProperty instance = null;
    private ApplicationProperty(){}
    public static synchronized ApplicationProperty getInstance(){
        if (instance == null){
            instance = new ApplicationProperty();
            return instance;
        } else {
            return instance;
        }
    }
    public Properties getProperty(){
        try {
            String file = getClass().getClassLoader().getResource("application.properties").getFile();
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            System.out.println(in.toString());
            this.prop.load(in);
            in.close();
        } catch (Exception e){
            e.printStackTrace();
        }
        return this.prop;
    }

    public static void main(String[] args){
        ApplicationProperty appConfig = ApplicationProperty.getInstance();
        Properties prop = appConfig.getProperty();
        System.out.println(prop.getProperty("hello"));
    }
}
