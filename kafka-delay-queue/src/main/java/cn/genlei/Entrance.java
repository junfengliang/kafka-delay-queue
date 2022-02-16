package cn.genlei;

import java.util.Properties;

public class Entrance {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers",args[0]);
        properties.put("group.id", args[1]);
        properties.put("topics", args[2]);

        DelayQueue delayQueue = new DelayQueue(properties);
        delayQueue.start();
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

