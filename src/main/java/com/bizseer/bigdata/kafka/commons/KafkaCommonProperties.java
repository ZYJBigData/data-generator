package com.bizseer.bigdata.kafka.commons;

import lombok.Data;

import java.util.Collection;
import java.util.Collections;

@Data
public class KafkaCommonProperties {

    /**
     * Kafka主机
     */
    private String kafkaHost = "10.0.60.141:9092,10.0.60.142:9092,10.0.60.143:9092";

    /**
     * 生产者：要求leader请求结束前收到的确认次数，来控制发送数据的持久化
     * 消息确认：
     * 0：生产者不等待服务器确认，此时retry参数不生效
     * 1：leader写入记录到log，不会等待follower的确认即向生产者发送通知
     * all：leader等待所有副本通知，然后向生产者发送通知，保证所有数据落盘到所有副本，功能同设置为-1
     */
    private String ack = "all";

    /**
     * 生产者重试次数
     */
    private Integer retryTimes = 1;

    /**
     * 生产者：向同一分区发送打包发送的数据量，单位：bytes，默认16384bytes=16K
     */
    private Integer batchSize = 16384;

    /**
     * 生产者：批量发送消息的间隔时间（延迟时间），单位：毫秒
     */
    private Integer lingerMs = 1;

    /**
     * 生产者：可以使用的最大缓存空间，单位：bytes，默认33554432bytes=32M.
     */
    private Integer bufferMemory = 33554432;

    /**
     * 生产者：键编码器
     */
    private String keyEncoder = "org.apache.kafka.common.serialization.StringSerializer";

    /**
     * 生产者：值编码器
     */
    private String valueEncoder = "org.apache.kafka.common.serialization.StringSerializer";

    /**
     * 消费者：消费topic的组ID
     */
    private String groupId = "my-group-id";

    /**
     * 消费者：后台定期提交offset
     */
    private String autoCommit = "true";

    /**
     * 消费者提交offset的时间间隔：单位：毫秒，当enable.auto.commit为true时生效
     */
    private String autoCommitIntervalMs = "1000";

    /**
     * 消费者：键解码器
     */
    private String keyDecoder = "org.apache.kafka.common.serialization.StringDeserializer";

    /**
     * 消费者：值解码器
     */
    private String valueDecoder = "org.apache.kafka.common.serialization.StringDeserializer";

    /**
     * 消费者：重启后配置offset
     * earliest：消费者恢复到当前topic最早的offset
     * latest：消费者从最新的offset开始消费
     * none：如果消费者组没找到之前的offset抛出异常
     * 其他任何值都会抛出异常
     */
    private String autoOffsetReset = "latest";

    /**
     * TOPIC
     */
    private Collection<String> topic = Collections.singleton("data_platform_metric");

    public KafkaCommonProperties() {

    }

    public KafkaCommonProperties(String kafkaHost, String ack, Integer retryTimes, Integer batchSize, Integer lingerMs, Integer bufferMemory, String keyEncoder, String valueEncoder, String groupId, String autoCommit, String autoCommitIntervalMs, String keyDecoder, String valueDecoder, String autoOffsetReset, Collection<String> topic) {
        this.kafkaHost = kafkaHost;
        this.ack = ack;
        this.retryTimes = retryTimes;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.bufferMemory = bufferMemory;
        this.keyEncoder = keyEncoder;
        this.valueEncoder = valueEncoder;
        this.groupId = groupId;
        this.autoCommit = autoCommit;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.autoOffsetReset = autoOffsetReset;
        this.topic = topic;
    }
// 省略setter和getter及toString()  
}
