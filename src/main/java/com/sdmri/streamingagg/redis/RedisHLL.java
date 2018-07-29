package com.sdmri.streamingagg.redis;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class RedisHLL {

    private static final Jedis JEDIS = new Jedis();

    private RedisHLL(){
    }

    public static void addValuesForABucket(String userId, String bucketId, String...values){
        JEDIS.pfadd(getUserBucketId(userId, bucketId),values);
    }

    public static void getCountForOneBucket(String key){
        JEDIS.pfcount(key);
    }

    public static Long getCountForMultipleBuckets(String userId, String start, String end){
        final String merged_key = getUserMergedBucketId(userId, start, end);
        if (!JEDIS.exists(merged_key)) {
            List<String> existingKeys = new ArrayList<>();
            int index = Integer.valueOf(start);
            while( index <= Integer.valueOf(end)) {
                final String bucketKey = getUserBucketId(userId, String.valueOf(index));
                if(JEDIS.exists(bucketKey)) {
                    existingKeys.add(bucketKey);
                }
            }
            JEDIS.pfmerge(merged_key, existingKeys.toArray(new String[existingKeys.size()]));
        }
        return JEDIS.pfcount(merged_key);
    }

    private static String getUserBucketId(String userId, String bucket_id){
        return "user_" + userId + "_" + bucket_id;
    }

    private static String getUserMergedBucketId(String userId,
                                                String startBucketId, String endBucketId){
        return "user_" + userId + "_" + startBucketId + "_" + endBucketId;
    }
}
