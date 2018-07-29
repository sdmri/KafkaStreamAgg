package com.sdmri.streamingagg

import com.sdmri.streamingagg.datamodel.Event;
import com.sdmri.streamingagg.flink.CustomAssignerWithPeriodicWatermarks;
import com.sdmri.streamingagg.flink.KafkaMessageDeserializationSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class StreamingApplication {
    public static void main(String args[]) throws Exception{
        Properties properties = new Properties();
        InputStream input = new FileInputStream("mre.properties");
        properties.load(input);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumerBase consumer =new FlinkKafkaConsumer010((String) properties.get("topics"),
                new KafkaMessageDeserializationSchema(), properties).assignTimestampsAndWatermarks(
                new CustomAssignerWithPeriodicWatermarks()
        );

        consumer.setStartFromLatest();
        DataStream<Event> kafkaStream = env.addSource(consumer);

        WindowedStream<Tuple2<String, String>, Tuple, TimeWindow> bucketedWindowedStream =
                kafkaStream.
                        flatMap(new Splitter()).
                        keyBy(0).
                        timeWindow(
                                Time.seconds(20)
                        );

        bucketedWindowedStream.apply(new WindowFunction<Tuple2<String,String>, Tuple3<String,Integer,Set<String>>,
                Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow timeWindow,
                              Iterable<Tuple2<String, String>> iterable,
                              Collector<Tuple3<String,Integer,Set<String>>> collector) throws Exception {
                Set<String> hotels = new HashSet<>();

                String userId = "";
                for (Tuple2<String, String> v : iterable) {
                    hotels.add(v.f1);
                    userId = v.f0;
                }
                Long start = timeWindow.getStart();
                collector.collect(new Tuple3<>(userId, hotels.size(), hotels));
                RedisHLL.addValuesForABucket(userId, start.toString(), hotels.toArray(new String[hotels.size()]));
            }
        }).print();

        env.execute();
    }

    public static class Splitter implements FlatMapFunction<Event, Tuple2<String, String>> {
        @Override
        public void flatMap(Event event, Collector<Tuple2<String, String>> out) throws Exception {
            out.collect(new Tuple2<>(event.key, event.payload));
        }
    }

}

