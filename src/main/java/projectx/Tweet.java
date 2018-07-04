package projectx;

import java.io.Serializable;

/**
 * Javadoc pending.
 */
class Tweet implements Serializable {
    private final long timestamp;
    private final String text;

    Tweet(long timestamp, String text) {
        this.timestamp = timestamp;
        this.text = text;
    }

    long timestamp() {
        return timestamp;
    }

    String text() {
        return text;
    }
}
