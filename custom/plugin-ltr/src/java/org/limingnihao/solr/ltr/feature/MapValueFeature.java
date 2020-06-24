package org.limingnihao.solr.ltr.feature;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.ltr.feature.ValueFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 自定义mapValueFeature
 * 用于查表函数在ltr feature中获取偏移量的值
 *
 * @author shiming.li
 * @date 2020-03-23
 */
public class MapValueFeature extends ValueFeature {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private String key;
    private int offset;

    public MapValueFeature(String name, Map<String, Object> params) {
        super(name, params);
        try {
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Override
    public Object getValue() {
        return paramsToMap();
    }

    @Override
    public LinkedHashMap<String, Object> paramsToMap() {
        final HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("key", this.key);
        valueMap.put("offset", this.offset);

        final LinkedHashMap<String, Object> params = super.paramsToMap();
        params.put("value", valueMap);
        return params;
    }

    @Override
    public void setValue(Object value) {
        if (value != null && value instanceof Map) {
            Map<String, Object> params = (Map<String, Object>) value;
            this.key = params.get("key").toString();
            this.offset = Integer.parseInt(params.get("offset").toString());
        }
    }

    public String getKey() {
        return key;
    }

    public int getOffset() {
        return offset;
    }

    public MapValueSource toValueSource() {
        return new MapValueSource(this.key, this.offset);
    }

    /**
     * MapValueSource
     */
    public class MapValueSource extends ValueSource {

        private String key = "";
        private Integer offset = 0;

        public MapValueSource(String key, int offset) {
            this.key = key;
            this.offset = offset;
        }

        @Override
        public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
            return new MapValueFunction(this.key, this.offset);
        }

        @Override
        public boolean equals(Object o) {
            if (this.getClass() != o.getClass()) {
                return false;
            }
            MapValueSource other = (MapValueSource) o;
            return this.key.equals(other.key) && this.offset.intValue() == other.offset.intValue();
        }

        @Override
        public int hashCode() {
            return this.key.hashCode() + this.offset.hashCode();
        }

        @Override
        public String description() {
            return "MapValueSource[key=" + this.key + ", offset=" + this.offset + "]";
        }

        @Override
        public String toString() {
            return "MapValueSource{" +
                    "key='" + key + '\'' +
                    ", offset=" + offset +
                    '}';
        }
    }

    /**
     * MapValueFunctionValues
     */
    public class MapValueFunction extends FunctionValues {

        private String key;
        private int offset;

        public MapValueFunction(String key, int offset) {
            this.key = key;
            this.offset = offset;
        }

        @Override
        public float floatVal(int doc) throws IOException {
            return 0f;
        }

        @Override
        public String strVal(int doc) throws IOException {
            return "";
        }

        public String getKey() {
            return key;
        }

        public int getOffset() {
            return offset;
        }

        @Override
        public String toString(int doc) throws IOException {
            return "MapValueFunction[key=" + this.key + ", offset=" + this.offset + "]";
        }

    }
}
