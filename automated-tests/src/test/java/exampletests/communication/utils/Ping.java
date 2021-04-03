package exampletests.communication.utils;

import java.io.Serializable;

public class Ping implements Serializable {
    private final int pingValue;

    Ping(int pingValue) {
        this.pingValue = pingValue;
    }

    public int getPingValue() {
        return pingValue;
    }
}
