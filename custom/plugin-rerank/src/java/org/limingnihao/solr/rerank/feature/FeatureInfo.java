package org.limingnihao.solr.rerank.feature;

/**
 * feature info
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class FeatureInfo {
    private String name;
    private float value;
    private boolean used;
    private int index;

    public FeatureInfo(String name, float value, boolean used, int index) {
        this.name = name;
        this.value = value;
        this.used = used;
        this.index = index;
    }

    public FeatureInfo(FeatureInfo other) {
        this.name = other.name;
        this.value = other.value;
        this.used = other.used;
        this.index = other.index;
    }

    @Override
    public String toString() {
        return "FeatureInfo{" +
                "name='" + name + '\'' +
                ", value=" + value +
                ", used=" + used +
                ", index=" + index +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public boolean isUsed() {
        return used;
    }

    public void setUsed(boolean used) {
        this.used = used;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
