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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.hive.HiveContext;
//import org.apache.spark.sql.hive.api.java.HiveContext;

/**
 * Created by zhipj on 7/22/2016.
 */
public class FlumePollingHiveStore {
	private FlumePollingHiveStore() {
	}

	private static final String tableName = "log";

	private static final Logger logger = Logger.getLogger("Access");

	// Example Apache log line:
	//   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
	private static final String LOG_ENTRY_PATTERN =
			// 1:host  2:identity  3:user  4:time
			// 5:method  6:request  7:protocal   8:rescode  9:size  10:referer  11:agent
			"^(\\S+) (\\S+) (\\S+) \\[([\\w-]+\\s[\\w:.]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+|-) \"(\\S+)\" \"([\\w/.\\(\\s;:\\),\\-\\+]+)\"";

	private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

	public static String[] parseLogRecord(String record) {
		Matcher m = PATTERN.matcher(record);
		if (!m.find()) {
			logger.log(Level.WARNING, "Cannot parse record: " + record);
			throw new RuntimeException("Error parsing record");
		}

		return new String[] { m.group(1), m.group(2), m.group(3), m.group(4), m.group(5),
				m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11)};
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: FlumePollingHiveStore  <host> <port>");
			System.exit(1);
		}

		String host = args[0];
		int port = Integer.parseInt(args[1]);

		Duration batchInterval = new Duration(2000);
		SparkConf sparkConf = new SparkConf().setAppName("FlumePollingHiveStore ");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
				FlumeUtils.createPollingStream(ssc, host, port);

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		final HiveContext sqlContext = new HiveContext(sc);

		// The schema is encoded in a string
		String schemaString = "host identity user time method request protocal rescode size referer agent";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		final StructType schema = DataTypes.createStructType(fields);

		// Generate records from flume stream
		// Essentially 1 DStream contains 1 RDD.
		flumeStream.foreachRDD(new Function<JavaRDD<SparkFlumeEvent>, Void>() {
			public Void call(JavaRDD<SparkFlumeEvent> sparkFlumeEventJavaRDD) throws Exception {
				System.out.println(">> Interate through flumeStream.");
				JavaRDD<Row> rowRDD = sparkFlumeEventJavaRDD.map(new Function<SparkFlumeEvent, Row>() {
					public Row call(SparkFlumeEvent e) throws Exception {
						String record = new String(e.event().getBody().array());
						String[] fields = parseLogRecord(record);
						return RowFactory.create(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5],
								fields[6], fields[7], fields[8], fields[9], fields[10], fields[11]);
					}
				});
				DataFrame recordDF = sqlContext.createDataFrame(rowRDD, schema);
				recordDF.write().mode("append").saveAsTable(tableName);
				return null;
			}
		});
	}
}
