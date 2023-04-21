package com.bizseer.bigdata.kafka.test;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Properties;

@NoArgsConstructor
@Getter
public class KafkaGetConfig {


    public String topic = "lyy_test_1p";
    public String groupId = "data_platform_metric_group";
//    public String bootstrapServers = "node001:9092,node003:9092";
    public String bootstrapServers = "10.0.90.74:9000,10.0.90.75:9000,10.0.90.76:9000";
//    public String bootstrapServers = "10.0.33.33:9000,10.0.33.34:9000,10.0.33.35:9000";
//    public String bootstrapServers = "10.0.110.114:9092";
//    public String bootstrapServers = "bs-data-arch-juggle-pts-001:9092";

    public KafkaGetConfig(String topic) {
        this.topic = topic;
    }

    public KafkaGetConfig(String topic, String groupId) {
        this.topic = topic;
        this.groupId = groupId;
    }

    public Properties getKafkaConfig() {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

//        properties.put("security.protocol", "SASL_PLAINTEXT");
//        properties.put("sasl.mechanism", "GSSAPI");
//        properties.put("sasl.kerberos.service.name", "kafka");

        return properties;
    }

}
