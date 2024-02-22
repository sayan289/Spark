package org.spark.broadcastvariable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class broadcastvariable {
    public static void main(String ars[]) {
        SparkSession spark = SparkSession.builder()
                .appName("Broadcast variable !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Map<String,String> tickerStockName=new HashMap<>(){{
            put("AAPL","Apple Inc");
            put("META","Meta platform Inc");
            put("GOOGL","Alphabet Inc");
        }};
        Map<String,Double> tickerLastClosePrice=new HashMap<>(){{
            put("GOOGL",200.5D);
            put("AAPL",100.1D);
            put("META",300.3D);
        }};
        Broadcast<Map<String, String>> broadcasttickerStockName = sc.broadcast(tickerStockName);
        Broadcast<Map<String, Double>> broadcasttickerLastClosePrice = sc.broadcast(tickerLastClosePrice);
        try{
            List<String> tickers = List.of("AAPL", "META", "GOOGL");
            JavaRDD<String> myrdd = sc.parallelize(tickers);
            JavaRDD<String> lastClosePriceRdd=myrdd.map(ticker->{
                String tickerfullname = broadcasttickerStockName.value().get(ticker);
                Double tickerClosePrice = broadcasttickerLastClosePrice.value().get(ticker);
                return String.format("Ticker= "+ticker+" TickerFullname ="+tickerfullname+" TickerPrice ="+tickerClosePrice);
            });
            lastClosePriceRdd.collect().forEach(System.out::println);
        }
        finally {
            broadcasttickerLastClosePrice.destroy(true);
            broadcasttickerStockName.destroy(true);
        }

    }
}
