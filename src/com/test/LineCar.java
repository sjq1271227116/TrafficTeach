package com.test;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class LineCar {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]").setAppName("linecar");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = context.textFile("./monitor_flow_action");
        rdd1 = rdd1.cache();

        //2018-06-27	0007	00536	京R66884	2018-06-27 11:30:25	30	41	08

        //2018-06-27	0000	08596	京R668842018-06-27 11:26:50	82	39	01
        //2018-06-27	0000	08596	京R66884	2018-06-27 11:26:50	82	39	01
        //2018-06-27	0000	08596	京R66884	2018-06-27 11:26:50	82	39	01
        JavaPairRDD<String, String> mapToPair1 = rdd1.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] strings = s.split("\t");
                String m_id = strings[1];
                String row = s;
                return new Tuple2<>(m_id, row);
            }
        });

        JavaPairRDD<String, String> filters = mapToPair1.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._1.equals("0001");
            }
        });

        JavaRDD<String> carMap = filters.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String[] strings2 = stringStringTuple2._2.split("\t");

                return strings2[3];
            }
        }).distinct();


        carMap=carMap.cache();

        List<String> carList = carMap.collect();
        //将0001卡扣获得的car车牌号广播出去
        Broadcast<List<String>> broadcast = context.broadcast(carList);

        JavaPairRDD<String, String> carRow = rdd1.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] strings = s.split("\t");


                return new Tuple2<>(strings[3], s);
            }
        });

        JavaPairRDD<String, String> filterCarRow = carRow.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                List<String> carList = broadcast.getValue();
                String s = stringStringTuple2._1;
                return carList.contains(s);
            }
        });

        JavaPairRDD<String, Iterable<String>> carRowGroupKey = filterCarRow.groupByKey();

        carRowGroupKey.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                Iterator<String> iterator = stringIterableTuple2._2.iterator();
                ArrayList<String> arrayList = new ArrayList<>();
                String mid22=stringIterableTuple2._1;

                while (iterator.hasNext()){
                    String[] split = iterator.next().split("\t");
                    String mid = split[1];
                    String timeId = split[4];
                    arrayList.add(timeId+","+mid);

                }

                arrayList.sort(new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        String[] strings = o1.split(",");
                        String[] strings1 = o2.split(",");
                        return strings[0].compareTo(strings1[0]);
                    }
                });

                ArrayList<String> stringLast = new ArrayList<>();
                for(String s22:arrayList){


                    String[] split22 = s22.split(",");


                    stringLast.add(split22[1]);
                }

                System.out.println(mid22+":"+stringLast.toString());





            }
        });


    }
}
