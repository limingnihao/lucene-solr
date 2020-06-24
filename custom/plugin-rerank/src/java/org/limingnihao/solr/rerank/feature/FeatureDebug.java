package org.limingnihao.solr.rerank.feature;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

public class FeatureDebug {
    private String name;
    private long time = 0;
    private long total = 0;
    private long cost = 0;
    private FeatureDebug[] children;

    public FeatureDebug(String name) {
        this.name = name;
    }

    public void inc(long time, long total) {
        this.time += time;
        this.total += total;
    }

    public void inc(long time) {
        this.time += time;
    }

    public void incCost(long cost) {
        this.cost += cost;
    }

    public String getName() {
        return name;
    }

    public long getTime() {
        return time;
    }


    public long getTotal() {
        return total;
    }


    public long getCost() {
        return cost;
    }


    public FeatureDebug[] getChildren() {
        return children;
    }

    public void setChildren(FeatureDebug[] children) {
        this.children = children;
    }

    public NamedList asNamedList() {
        NamedList<Object> m = new SimpleOrderedMap<>();
        m.add("time", (long) (time / 1000000.0));
        m.add("total", total);
        m.add("cost", cost);

        if (this.children != null) {
            NamedList<Object> children = new SimpleOrderedMap<>();
            for (FeatureDebug f : this.children) {
                children.add(f.getName(), f.asNamedList());
            }
            m.add("children", children);
        }
        return m;
    }

}
