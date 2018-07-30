package com.sdmri.streamingagg;

import com.sdmri.streamingagg.datamodel.Event;
import com.sdmri.streamingagg.flink.CustomAssignerWithPeriodicWatermarks;
import com.sdmri.streamingagg.flink.KafkaMessageDeserializationSchema;
import com.sdmri.streamingagg.redis.RedisHLL;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
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

    private static final int WINDOW_WIDTH_SECONDS = 20;
    public static void main(String args[]) throws Exception{
        Properties properties = new Properties();
        InputStream input = new FileInputStream("streaming.properties");
        properties.load(input);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumerBase consumer =new FlinkKafkaConsumer010((String) properties.get("topics"),
                new KafkaMessageDeserializationSchema(), properties).assignTimestampsAndWatermarks(
                new CustomAssignerWithPeriodicWatermarks()
        );

        consumer.setStartFromLatest();
        DataStream<Event> kafkaStream = env.addSource(consumer);

        WindowedStream<Event, String, TimeWindow> windowedStream = kafkaStream.keyBy(new EventKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_WIDTH_SECONDS)));


        windowedStream.apply(new WindowFunction<Event, Tuple3<String, Integer, Set<String>>, String, TimeWindow>() {
            @Override
            public void apply(String tuple, TimeWindow timeWindow,
                              Iterable<Event> iterable, Collector<Tuple3<String, Integer, Set<String>>> collector) throws Exception {
                Set<String> hotels = new HashSet<>();

                String userId = "";
                for (Event v : iterable) {
                    hotels.add(v.payload);
                    userId = v.key;
                }
                Long start = timeWindow.getStart();
                collector.collect(new Tuple3<>(userId, hotels.size(), hotels));
                RedisHLL.addValuesForABucket(userId, start.toString(), hotels.toArray(new String[hotels.size()]));
            }
        }).print();

        env.execute();
    }

    public static class EventKeySelector implements KeySelector<Event, String>{

        @Override
        public String getKey(Event e) throws Exception {
            return e.key;
        }
    }

}

