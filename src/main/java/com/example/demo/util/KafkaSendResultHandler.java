package com.example.demo.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;


@Slf4j
public class KafkaSendResultHandler implements ProducerListener<String, String> {
    @Override
    public void onSuccess(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata) {
//        System.out.println("success...........");
    }

    @Override
    public void onError(ProducerRecord<String, String> producerRecord, Exception exception) {
//        System.out.println("fail.............");
    }
}
