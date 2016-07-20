package org.apache.spark.examples.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

/**
 *  Produces a count of events received from Flume.
 *
 *  This should be used in conjunction with an AvroSink in Flume. It will start
 *  an Avro server on at the request host:port address and listen for requests.
 *  Your Flume AvroSink should be pointed to this address.
 *
 *  Usage: FlumePollingEventCount  <host> <port>
 *    <host> is the host the Flume receiver will be started on - a receiver
 *           creates a server and listens for flume events.
 *    <port> is the port the Flume receiver will listen on.
 *
 *  To run this example:
 *     `$ bin/run-example org.apache.spark.examples.streaming.FlumePollingEventCount  <host> <port>`
 */
public final class FlumePollingEventCount  {
  private FlumePollingEventCount () {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: FlumePollingEventCount  <host> <port>");
      System.exit(1);
    }

    //StreamingExamples.setStreamingLogLevels();

    String host = args[0];
    int port = Integer.parseInt(args[1]);

    Duration batchInterval = new Duration(2000);
    SparkConf sparkConf = new SparkConf().setAppName("FlumePollingEventCount ");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
    JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
      FlumeUtils.createPollingStream(ssc, host, port);

    flumeStream.count().map(new Function<Long, String>() {
      @Override
      public String call(Long in) {
        return "Received " + in + " flume events.";
      }
    }).print();
  
    ssc.start();
    ssc.awaitTermination();
  }
}
