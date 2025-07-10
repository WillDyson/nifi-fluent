package com.cloudera.ps.example.fluentReader;

import java.util.HashMap;
import java.util.Map;

public class FluentRecord {
    public String tag;
    public long epoch;
    public Map<String, String> entries;

    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<String, Object>();

        map.put("tag", tag);
        map.put("epoch", epoch);
        map.put("entries", entries);

        return map;
    }

    @Override
    public String toString() {
        return "FluentRecord [tag=" + tag + ", epoch=" + epoch + ", entries=" + entries + "]";
    }
}