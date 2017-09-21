package com.adatafun.dataprocess.mysql;

import com.adatafun.conf.ApplicationProperty;
import com.adatafun.model.RestaurantUser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple7;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by yanggf on 2017/9/19.
 */
public class MySQLDB {
    private static Logger logger = Logger.getLogger("MySQLDB");
    public static void main(String[] args){
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
        try{
            String url = "jdbc:mysql://localhost:3306/recommentation_local";
            String table = "restaurant_order_detail2";
            Properties prop = new Properties();
            prop.setProperty("dbtalbe",table);
            prop.setProperty("user","root");
            prop.setProperty("password","456789");
            Dataset orderDS = spark.read().jdbc(url,table,prop);
            Dataset tbOrderDS = spark.read().jdbc(url,"tb_order",prop);
            Dataset restaurantDS = spark.read().jdbc(url,"itd_restaurant",prop);
            Dataset restDS = restaurantDS.filter(restaurantDS.col("fd_lg").equalTo("zh-cn"));

            Dataset togetherDS = orderDS.join(restDS, orderDS.col("fd_restaurant_code").equalTo(restDS.col("fd_code")),"left_outer");
            Dataset allDS = togetherDS.join(tbOrderDS, orderDS.col("fd_code").equalTo(tbOrderDS.col("order_no")),"left_outer");

            List<Column> listColumns = new ArrayList<Column>();
            listColumns.add(tbOrderDS.col("user_id"));//用户id
            listColumns.add(orderDS.col("fd_restaurant_code"));//餐馆code
            listColumns.add(orderDS.col("fd_code"));//订单code
            listColumns.add(orderDS.col("ordertime"));//订单时间
            listColumns.add(orderDS.col("cprice"));//订单金额
            listColumns.add(restDS.col("fd_inspection"));//餐馆位置
            listColumns.add(orderDS.col("fd_class"));//餐馆类型

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumns).toSeq();
            Dataset resultDS = allDS.select(seqCol);//裁剪后的数据
            Dataset resultNull = resultDS.na().drop();//null值数据删除掉

            JavaRDD<Row> rowRDD = resultNull.toJavaRDD();

            JavaRDD<Tuple7<String,String,String,Date,String,String,String>> resultRDD = rowRDD.map(new Function<Row, Tuple7<String, String, String, Date, String, String, String>>() {
                public Tuple7<String, String, String, Date, String, String, String> call(Row row) throws Exception {
                    String str1 = String.valueOf(row.getAs(0));
                    String str2 = row.getAs(1);
                    String str3 = row.getAs(2);
                    Date date4 = row.getAs(3);
                    String str5 = String.valueOf(row.getAs(4));
                    String str6 = row.getAs(5);
                    String str7 = row.getAs(6);
                    Tuple7<String,String,String,Date,String,String,String> tpl7 = new Tuple7<String,String,String,Date,String,String,String>(
                            str1,str2,str3,date4,str5,str6,str7
                    );
                    return tpl7;
                }
            });

            JavaPairRDD<Tuple3<String,String,String>, Integer> idRDD = resultRDD.mapToPair(new PairFunction<Tuple7<String, String, String, Date, String, String, String>, Tuple3<String, String, String>, Integer>() {
                public Tuple2<Tuple3<String, String, String>, Integer> call(Tuple7<String, String, String, Date, String, String, String> strTuple7) throws Exception {
                    String id = strTuple7._1()+ strTuple7._2();
                    Tuple3<String,String,String> tpl3 = new Tuple3<String, String, String>(
                            id, strTuple7._1(), strTuple7._2()
                    );
                    Tuple2<Tuple3<String, String, String>, Integer> tplPair = new Tuple2<Tuple3<String, String, String>, Integer>(tpl3,1);
                    return tplPair;
                }
            });
            JavaPairRDD< Tuple3<String,String,String>,Integer > numRDD = idRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });
            JavaRDD<RestaurantUser> restRDD = numRDD.map(new Function<Tuple2<Tuple3<String, String, String>, Integer>, RestaurantUser>() {
                public RestaurantUser call(Tuple2<Tuple3<String, String, String>, Integer> tuple3IntegerTuple2) throws Exception {
                    RestaurantUser rest = new RestaurantUser();
                    rest.setId(tuple3IntegerTuple2._1()._1());
                    rest.setUserId(tuple3IntegerTuple2._1()._2());
                    rest.setRestaurantCode(tuple3IntegerTuple2._1()._3());
                    rest.setConsumptionNum(tuple3IntegerTuple2._2());
                    return rest;
                }
            });

            SQLContext context = new SQLContext(spark);
            Dataset ds = context.createDataFrame(restRDD, RestaurantUser.class);
            EsSparkSQL.saveToEs(ds,"user/userRest");
            System.out.print(restRDD.count());
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
