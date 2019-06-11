package com.example;

import com.example.config.MyProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ApplicationTests {

    @Autowired
    MyProducer producer;

    @Test
    public void contextLoads() {
//        long begin = System.currentTimeMillis();
//        for (int i = 0; i < 100; i++) {
////            producer.sendToKafkaAsync(new ProducerRecord<>("topic1", "" + i, "val" + i));
//            producer.sendToKafkaSync(new ProducerRecord<>("topic1", "" + i, "val" + i));
//        }
//        long end = System.currentTimeMillis();
//        System.out.println("发送一百条消息花费的时间:" + (end - begin));// 异步53ms 同步289ms
        testTransaction();
        System.out.println(">>> end");
    }

    @Transactional
    public void testTransaction() {
        for (int i = 0; i < 100; i++) {
            System.out.println(String.format("%3d", i) + "---------------------");
            producer.sendToKafkaAsync(new ProducerRecord<>("thing2", "hello-" + String.format(
                    "%3d", i)));

            producer.sendToKafkaSync(new ProducerRecord<>("thing2", "world-" + String.format("%3d", i)));
            System.out.println("------------------------");
        }
    }

}
