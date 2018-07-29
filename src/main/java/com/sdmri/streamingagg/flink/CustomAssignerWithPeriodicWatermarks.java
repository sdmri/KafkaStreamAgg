package com.sdmri.streamingagg.flink;

import com.sdmri.streamingagg.datamodel.Event;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class CustomAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Event> {

    private final long maxOutOfOrderness = 10000; // 10 seconds

    private long currentMaxTimestamp;

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Event event, long kafkaTimestamp) {
        event.timestamp = kafkaTimestamp;
        event.epoch = kafkaTimestamp/1000;
        currentMaxTimestamp = Math.max(kafkaTimestamp, currentMaxTimestamp);
        return kafkaTimestamp;
    }
}