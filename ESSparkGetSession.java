package com.adatafun.conf;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Created by yanggf on 2017/9/21.
 */
public class ESMysqlSpark {
    public static SparkSession getSession(){
        SparkConf conf = new SparkConf().setAppName("testEs").setMaster("local");
        Properties props = ApplicationProperty.getInstance().getProperty();
        conf.set("es.index.auto.create",props.getProperty("es.index.auto.create"));
        conf.set("es.resource",props.getProperty("es.resource"));
        conf.set("es.nodes",props.getProperty("es.nodes"));
        conf.set("es.port",props.getProperty("es.port"));
        conf.set("es.nodes.wan.only",props.getProperty("es.nodes.wan.only"));
        conf.set("es.mapping.id",props.getProperty("es.mapping.id"));
        conf.set("es.write.operation",props.getProperty("es.write.operation"));
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config(conf)
                .getOrCreate();
        return spark;
    }
    public static Properties getMysqlConf(){
        Properties appConf = ApplicationProperty.getInstance().getProperty();
        Properties propMysql = new Properties();
        propMysql.setProperty("url",appConf.getProperty("mysqlUrl"));
        propMysql.setProperty("user",appConf.getProperty("user"));
        propMysql.setProperty("password",appConf.getProperty("password"));
        return propMysql;
    }
}
