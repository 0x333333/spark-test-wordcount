package spark.examples;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import org.apache.spark.streaming.flume.*;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


public final class JavaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf()
                .setAppName("JavaWordCount")
                .set("spark.executor.memory", "16M")
                .set("spark.rdd.compress", "false");

    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(args[0], 1);

    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) {
        return Arrays.asList(SPACE.split(s));
      }
    });

    words.persist(StorageLevel.MEMORY_AND_DISK_SER());

    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
        // System.out.println(s);
        return new Tuple2<String, Integer>(s, 1);
      }
    });

    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    });

    System.out.println("\n-----------------");
    long startTime = System.nanoTime();
    System.out.println("Count: " + counts.keys().count());
    long endTime = System.nanoTime();
    System.out.println("Execution Time: " + (endTime - startTime)/1000000 + " ms");
    System.out.println("-----------------\n");
    // counts.saveAsTextFile(".\\result1\\");
  }

}
