package cn.genlei;

import cn.genlei.dto.DelayTopicDTO;
import cn.genlei.inner.DelayTopicRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DelayQueue {
    static Logger log = LoggerFactory.getLogger(DelayQueue.class);
    Properties properties;
    public DelayQueue(Properties properties){
        this.properties = properties;
    }
    public void start(){
        String topics = properties.getProperty("topics");
        List<DelayTopicDTO> list = parseDelay(topics);
        if(list==null || list.size()==0){
            log.warn("unable to get topics from config.");
        }
        int i=1;
        for(DelayTopicDTO dto:list){
            Thread thread = new Thread(new DelayTopicRunner(dto.getDelayMs(),dto.getTopic(),this.properties));
            thread.setName("kafka-delay-" + i++);
            thread.setDaemon(true);
            thread.start();
        }
    }

    private List<DelayTopicDTO> parseDelay(String topics) {
        List<DelayTopicDTO> list = new ArrayList<>();

        String[] arr = topics.split(";");
        for(String item : arr){
            String[] itemArr = item.split(",");
            if(itemArr.length<2){
                continue;
            }
            DelayTopicDTO dto = new DelayTopicDTO();
            dto.setDelayMs(Integer.parseInt(itemArr[0]));
            dto.setTopic(itemArr[1]);
            list.add(dto);
        }
        return list;
    }

}

