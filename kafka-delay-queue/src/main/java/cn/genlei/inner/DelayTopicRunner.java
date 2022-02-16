package cn.genlei.inner;

import cn.genlei.TopicReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DelayTopicRunner implements Runnable{
    static Logger log = LoggerFactory.getLogger(DelayTopicRunner.class);

    long delayMs;
    String topic;
    Properties properties;

    TopicReader topicReader;

    public DelayTopicRunner(long delayMs, String topic,Properties properties) {
        this.properties = properties;
        this.delayMs = delayMs;
        this.topic = topic;
        String topicReader = properties.getProperty("topic.reader");
        if(topicReader!=null){
            try {
                this.topicReader = (TopicReader)Class.forName(topicReader).newInstance();
            } catch (InstantiationException e) {
                log.error(e.getMessage());
            } catch (IllegalAccessException e) {
                log.error(e.getMessage());
            } catch (ClassNotFoundException e) {
                log.error(e.getMessage());
            }
        }
        if(this.topicReader==null){
            this.topicReader = new DefaultTopicReader();
        }
    }

    @Override
    public void run() {
        log.debug("start run topic:{},delay(ms):{}",this.topic,this.delayMs );
        Properties props = new Properties();
        props.put("bootstrap.servers", this.properties.getProperty("bootstrap.servers"));
        props.put("group.id", this.properties.getProperty("group.id"));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit","false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        Properties productProps = new Properties();
        productProps.put("bootstrap.servers", this.properties.getProperty("bootstrap.servers"));
        productProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        productProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(productProps);

        List<String> list = new ArrayList<>();
        list.add(topic);
        consumer.subscribe(list);

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                while (true){
                    long sleep = record.timestamp() + delayMs - System.currentTimeMillis();
                    if(sleep>0){
                        try {
                            Thread.sleep(sleep);
                        } catch (InterruptedException e) {
                            log.error(e.getMessage());
                        }
                        continue;
                    }
                    String targetTopic = this.topicReader.readTopic(record.value());
                    if(targetTopic==null || targetTopic.length()==0){
                        log.warn("unable to read topic from value:{}",record.value());
                        break;
                    }
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(targetTopic, record.value());
                    try {
                        producer.send(producerRecord);
                        log.debug("send {} to {}",record.value(),targetTopic);
                    }catch (RuntimeException e){
                        e.printStackTrace();
                    }
                    break;
                }
            }
            try {
                consumer.commitSync();
            }catch (RuntimeException e){
                log.error(e.getMessage());
            }
        }
    }
}

