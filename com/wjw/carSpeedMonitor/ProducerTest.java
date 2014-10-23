/**
 * Created by wjw on 14-10-19.
 */
package com.wjw.carSpeedMonitor;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

public class ProducerTest {
    static ProducerConfig config = null;

    static Producer<String, String> producer = null;
    static Properties props = new Properties();
    public static void init() {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list","192.168.0.8:9092");
        config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }
    public static void main(String[] args) {

        String filePath = "/home/wjw/gps-data";
        init();
        sendMsg("wjw","npu");
        producer.close();
    }

    public static void sendMsg(String msg) {
        //long runtime = new Date().getTime();
//        Random rnd = new Random();
//        String ip = "192.168.2." + rnd.nextInt(255);
//        KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
//        producer.send(data);
        sendMsg(msg,"page_visits");
    }
    public static void sendMsg(String msg, String topic) {
        Random rnd = new Random();
        String ip = "192.168.2." + rnd.nextInt(255);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
        producer.send(data);
    }
}

