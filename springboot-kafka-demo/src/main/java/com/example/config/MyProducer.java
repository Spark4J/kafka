package com.example.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class MyProducer {

    @Autowired
    private KafkaTemplate<String, String> template;

    // 异步非阻塞
    public void sendToKafkaAsync(final ProducerRecord<String, String> record) {
        ListenableFuture<SendResult<String, String>> future = template.send(record);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("发送消息成功: record={}, result={}", record, result);
            }

            @Override
            public void onFailure(Throwable ex) {
                log.info("发送消息失败: record={}, exception={}", record, ex.getMessage());
                ex.printStackTrace();
            }

        });
    }

    // 同步阻塞
    public void sendToKafkaSync(final ProducerRecord<String, String> record) {
        try {
            /*
            get(): broker返回不可恢复异常直接抛出,其他异常进行重试,失败超过一定次数抛出
             */
            template.send(record).get(10, TimeUnit.SECONDS);
            log.info("发送消息成功: record={}", record);
        } catch (Exception e) {
            log.info("发送消息失败: record={}, exception={}", record, e.getMessage());
            e.printStackTrace();
        }
    }

}
