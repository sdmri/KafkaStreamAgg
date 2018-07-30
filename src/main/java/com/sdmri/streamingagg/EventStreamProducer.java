package com.sdmri.streamingagg;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.SplittableRandom;

public class EventStreamProducer {

    public static void main(String args[]) throws Exception{
        final Producer<Long,String> producer = createProducer();
        try{
            int index = 1;
            while(true){
                int user_id = new SplittableRandom().nextInt(1, 5);
                int hotel_id = new SplittableRandom().nextInt(10001, 10020);
                final ProducerRecord<Long,String> record = new ProducerRecord<>("test", System.currentTimeMillis(),
                        user_id + "_" + hotel_id);
                producer.send(record).get();
                index++;
                if( index % 20 == 0 ) {
                    Thread.sleep(5 * 1000);
                }
            }
        }finally{
            producer.flush();
            producer.close();
        }

    }

    private static Producer<Long, String> createProducer() throws Exception {
        Properties props = new Properties();
        InputStream input = new FileInputStream("streaming.properties");
        props.load(input);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
