package cn.genlei.inner;

import cn.genlei.TopicReader;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DefaultTopicReader implements TopicReader {
    @Override
    public String readTopic(String value) {
        JsonObject jsonObject =(JsonObject) new JsonParser().parse(value);
        JsonElement element = jsonObject.get("topic");
        if(element!=null){
            return element.getAsString();
        }
        return null;
    }
    public static void main(String[] args){
        DefaultTopicReader reader = new DefaultTopicReader();
        System.out.println();
    }
}

