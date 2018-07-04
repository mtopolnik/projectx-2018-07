package datamodel;

import java.io.Serializable;

public class Tweet implements Serializable {
    private final long timestamp;
    private final String text;

    public Tweet(long timestamp, String text) {
        this.timestamp = timestamp;
        this.text = text;
    }

    public long timestamp() {
        return timestamp;
    }

    public String text() {
        return text;
    }
}
