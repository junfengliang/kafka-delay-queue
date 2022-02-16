package cn.genlei.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import cn.genlei.DelayQueue;
import java.util.Properties;

@SpringBootApplication
public class Application implements CommandLineRunner {

    @Value("${kafka.servers}")
    String servers;

    @Value("${kafka.group-id}")
    String groupId;

    @Value("${kafka.topics}")
    String topics;

    public static void main(String[] args){
        SpringApplication.run(Application.class);
    }


    @Override
    public void run(String... args) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers",servers);
        properties.put("group.id", groupId);
        properties.put("topics", topics);
        DelayQueue delayQueue = new DelayQueue(properties);
        delayQueue.start();
    }
}

