package spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.hive.HiveContext;
//import org.apache.spark.sql.hive.api.java.HiveContext;

public final class HiveStore  {
	private HiveStore () {
	}

	public static void main(String[] args) throws Exception {
//		if (args.length != 2) {
//			System.err.println("Usage: FlumePollingEventCount  <host> <port>");
//			System.exit(1);
//		}
//
//		//StreamingExamples.setStreamingLogLevels();
//
//		String host = args[0];
//		int port = Integer.parseInt(args[1]);
//
//		Duration batchInterval = new Duration(2000);
		SparkConf sparkConf = new SparkConf().setAppName("FlumePollingEventCount ");
//		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
//		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
//				FlumeUtils.createPollingStream(ssc, host, port);

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		HiveContext sqlContext = new HiveContext(sc);

		// Load a text file and convert each line to a JavaBean.
		List<String> data = Arrays.asList("zhipeng, 12", "jenny, 13");
		JavaRDD<String> people = sc.parallelize(data);

		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows.
		JavaRDD<Row> rowRDD = people.map(
				new Function<String, Row>() {
					public Row call(String record) throws Exception {
						String[] fields = record.split(",");
						return RowFactory.create(fields[0], fields[1].trim());
					}
				});

		// Apply the schema to the RDD.
		DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);

		// Register the DataFrame as a table.
		peopleDataFrame.registerTempTable("peoples");

		// SQL can be run over RDDs that have been registered as tables.
		DataFrame results = sqlContext.sql("SELECT * FROM peoples");

		// The results of SQL queries are DataFrames and support all the normal RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		List<String> names = results.javaRDD().map(new Function<Row, String>() {
			public String call(Row row) {
				return "Name: " + row.getString(0) + ", Age: " + row.getString(1);
			}
		}).collect();

		System.out.println(names.toString());

		results.write().mode("append").saveAsTable("people");
	}
}

