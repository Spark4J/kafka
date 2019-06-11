package com.example.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Configuration
public class MyListener {
    /*
    这里concurrency等于3, topicPartitions等于4,
    那么一个Container分配两个partition, 另外两个Container各分配一个partition
     */
    @KafkaListener(id = "myListener"/*, topicPartitions =
            {@TopicPartition(topic = "thing1", partitions = {"0", "1"}),
                    @TopicPartition(topic = "thing2", partitions = "0",
                            partitionOffsets = @PartitionOffset(partition = "1", initialOffset =
                                    "100"))// Note: 不要在partitions和partitionOffsets中指定同一个分区
            }*/, topics = "thing2", concurrency = "${listen.concurrency:3}")
    public void listen(ConsumerRecord<?, ?> record/*, Acknowledgment acknowledgment 手动模式才可以用*/) {
        log.info("收到消息: {}", record);
    }
}
