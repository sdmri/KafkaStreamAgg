# KafkaStreamAgg
Uses Flink and Redis to perform real time aggregations on streams reading from Kafka. It currently counts uniques within a
time window (20s) and uses HLL(HyperLogLog) to emit approximate unique counts over multiple intervals.

1. Source are Kafka topics which are ingested as Streams of events
2. Aggregation engine is Flink
3. Counts are stored in Redis
4. Approximate counts are served by HLL methods exposed by Redis

## Setup :-

1. Install Kafka (kafka_2.11-1.0.0)
2. Install Redis (4.0.10)
3. Start zookeeper  -> ./bin/zookeeper-server-start.sh ./config/zookeeper.properties
4. Start Kafka server -> ./bin/kafka-server-start.sh ./config/server.properties
5. Create a Kafka topic -> bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic test
6. Start redis -> redis-server /usr/local/etc/redis.conf
7. Run StreamingApplication.java (to run a Flink instance that fetches events from Kafka and builds aggregates)
8. Run EventStreamProducer.java (to push events to Kafka)
9. Finally run ApproxDistinctCounter.java. It will prompt you for a userid, which is a number between (1 to 4), and
the number of buckets in the past from now to fetch approximate unique count.

## How does it work :-

The premise of this project is to count the unique hotel ids visited by a user in a certain time interval.
The events being produced are of type <user_id>_<hotel_id>

StreamingApplication.java will keep printing out distinct count for each user for windows of 20 seconds each. eg.

      1> (2,8,[10019, 10002, 10001, 10015, 10004, 10017, 10005, 10016])
      2> (1,7,[10007, 10018, 10010, 10002, 10014, 10017, 10016])
      2> (3,10,[10007, 10009, 10011, 10002, 10001, 10015, 10004, 10003, 10017, 10016])
      1> (4,5,[10009, 10011, 10004, 10017, 10016])
      1> (2,12,[10008, 10019, 10007, 10009, 10013, 10002, 10001, 10015, 10014, 10003, 10006, 10016])
      2> (1,13,[10019, 10018, 10007, 10010, 10002, 10013, 10001, 10015, 10004, 10003, 10006, 10005, 10016])
      1> (4,13,[10019, 10007, 10018, 10009, 10011, 10010, 10013, 10002, 10012, 10001, 10015, 10004, 10005])

These values are persisted in Redis to calculate the approximate unique count when requested for.


