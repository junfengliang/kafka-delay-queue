package cn.genlei.dto;

public class DelayTopicDTO {
    long delayMs;
    String topic;

    public DelayTopicDTO(){

    }
    public DelayTopicDTO(long delayMs, String topic) {
        this.delayMs = delayMs;
        this.topic = topic;
    }

    public long getDelayMs() {
        return delayMs;
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}

