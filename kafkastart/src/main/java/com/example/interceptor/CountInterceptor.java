package com.example.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CountInterceptor implements ProducerInterceptor<String, String> {
    private int errorCount = 0;
    private int successCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // 统计失败或者成功的次数
        if (exception == null)
            successCount++;
        else
            errorCount++;
    }

    @Override
    public void close() {
        // 保存结果
        System.out.println("Successful sent: " + successCount);
        System.out.println("Failed sent: " + errorCount);

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
