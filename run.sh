for i in `seq 5`
do
  # echo $i
  ../spark/bin/spark-submit --class org.apache.spark.examples.JavaWordCount2 target/JavaWordCount-1.0-SNAPSHOT.jar linux2.words
done