package com.shsxt.spark.rtmroad;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class Test {

    public static void main(String[] args) {

        System.out.println("00800-80000-02000".compareTo("10800-9000-2000"));



//        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("partioner");
//
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//
//        JavaPairRDD<String, String> javaPairRDD = sc.parallelizePairs(Arrays.asList(
//                new Tuple2<>("1", "张三"),
//                new Tuple2<>("2", "李四"),
//                new Tuple2<>("3", "王五"),
//                new Tuple2<>("4", "小明"),
//                new Tuple2<>("5", "小红")
//        ));
//
//        javaPairRDD.reduceByKey(new Partitioner() {
//            @Override
//            public int getPartition(Object key) {
//
//                Integer aLong = Integer.valueOf(key.toString());
//                return  aLong % 3;
//            }
//
//            @Override
//            public int numPartitions() {
//                return 3;
//            }
//        }, new Function2<String, String, String>() {
//            @Override
//            public String call(String v1, String v2) throws Exception {
//                return null;
//            }
//        });
//
//        javaPairRDD.mapValues(new Function<String, Integer>() {
//            @Override
//            public Integer call(String v1) throws Exception {
//                return 1;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

    }
}
