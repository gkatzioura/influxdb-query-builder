package com.gkatzioura.influxdb.querybuilder;

public class Path {

    private final String[] segments;

    Path(String... segments) {
        this.segments = segments;
    }

    public String[] getSegments() {
        return segments;
    }

}
