package com.gkatzioura.influxdb.querybuilder;

public class RawString {

    private final String str;

    RawString(String str) {
        this.str = str;
    }

    @Override
    public String toString() {
        return str;
    }
}
