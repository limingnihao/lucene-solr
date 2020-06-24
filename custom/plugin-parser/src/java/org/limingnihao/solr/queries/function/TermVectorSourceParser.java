package org.limingnihao.solr.queries.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.queries.function.docvalues.StrDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TermVectorSourceParser extends ValueSourceParser {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String name = "term_vector";

    // distance 默认未匹配 返回值
    private static int DEFAULT_NOT_MATCH = -1;

    // 获取 position。 返回string类型
    private static final String FUNC_POSITION = "position";

    // 获取 offset。 返回string类型
    private static final String FUNC_OFFSET = "offset";

    // 获取 offset start。 返回string类型
    private static final String FUNC_OFFSET_START = "offset" + "_start";

    // 获取 offset end。 返回string类型
    private static final String FUNC_OFFSET_END = "offset" + "_end";


    // 距离 default = position。 返回int类型
    private static final String FUNC_DISTANCE = "distance";

    // 最大距离 position. max(position) - min(position)。 返回int类型
    private static final String FUNC_DISTANCE_POSITION = FUNC_DISTANCE + "_" + FUNC_POSITION;

    // 最大距离 offset。 max(endOffset) - min(startOffset)。 返回int类型
    private static final String FUNC_DISTANCE_OFFSET = FUNC_DISTANCE + "_" + FUNC_OFFSET;

    // 最大举距离 offset start。 max(startOffset) - min(startOffset)。 返回int类型
    private static final String FUNC_DISTANCE_OFFSET_START = FUNC_DISTANCE + "_" + FUNC_OFFSET + "_start";

    // 最大举距离 offset end。 max(endOffset) - min(endOffset)。 返回int类型
    private static final String FUNC_DISTANCE_OFFSET_END = FUNC_DISTANCE + "_" + FUNC_OFFSET + "_end";

    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        try {
            TermInfo tinfo = parseTerm(fp);
            String func = fp.parseArg();
            String def = fp.parseArg();
            if (StringUtil.isNotBlank(def)) {
                try {
                    DEFAULT_NOT_MATCH = Integer.parseInt(def);
                } catch (Exception e) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `def` is not numbers!");
                }
            }
            if (StringUtils.isBlank(tinfo.field)) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `field` is empty!");
            }
            if (StringUtils.isBlank(tinfo.val)) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `value` is empty!");
            }
            if (StringUtils.isBlank(func)) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `func` is empty!");
            } else if (!FUNC_POSITION.equals(func) && FUNC_OFFSET.equals(func) && FUNC_DISTANCE.equals(func) && FUNC_DISTANCE_POSITION.equals(func) && FUNC_DISTANCE_OFFSET.equals(func)) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `func` Supported: " + FUNC_POSITION + ", " + FUNC_OFFSET);
            } else {
                return new TermPositionValueSource(tinfo.field, tinfo.val, tinfo.termBytes, func);
            }
        } catch (IOException e) {
            throw new RuntimeException("caught exception in analyzer at funcdtion: " + name, e);
        }
    }

    public class TermPositionValueSource extends ValueSource {
        protected final String field;
        protected final String val;
        protected final BytesRef[] termBytes;
        protected final String func;

        public TermPositionValueSource(String field, String val, BytesRef termBytes[], String func) {
            this.field = field;
            this.val = val;
            this.termBytes = termBytes;
            this.func = func;
        }

        @Override
        public boolean equals(Object o) {
            if (this.getClass() != o.getClass()) {
                return false;
            }
            TermPositionValueSource other = (TermPositionValueSource) o;
            return this.field.equals(other.field) && this.val.equals(other.val);
        }

        @Override
        public int hashCode() {
            return getClass().hashCode() + field.hashCode() * 29 + val.hashCode() * 29 + func.hashCode();
        }

        @Override
        public String description() {
            return name + '(' + field + ',' + val + ',' + func + ')';
        }

        @Override
        public void createWeight(Map context, IndexSearcher searcher) throws IOException {
            context.put("searcher", searcher);
        }

        @Override
        public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
            FieldOptions allFields = new FieldOptions();
            if (func.startsWith(FUNC_DISTANCE)) {
                allFields.positions = FUNC_DISTANCE_POSITION.equals(func);
                allFields.offsets = func.startsWith(FUNC_DISTANCE_OFFSET);
                allFields.offsets_start = func.equals(FUNC_DISTANCE_OFFSET_START);
                allFields.offsets_end = func.equals(FUNC_DISTANCE_OFFSET_END);
                allFields.offsets = allFields.offsets_start || allFields.offsets_end;
                allFields.positions = allFields.offsets == true ? true : allFields.positions;
            } else {
                allFields.positions = FUNC_POSITION.equals(func);
                allFields.offsets = allFields.positions = allFields.offsets_start = allFields.offsets_end = FUNC_OFFSET.equals(func);
                if (allFields.offsets) {
                    allFields.offsets_start = true;
                    allFields.offsets_end = true;
                    allFields.positions = true;
                } else {
                    allFields.offsets_start = FUNC_OFFSET_START.equals(func);
                    allFields.offsets_end = FUNC_OFFSET_END.equals(func);
                    allFields.offsets = allFields.offsets_start || allFields.offsets_end;
                    allFields.positions = allFields.offsets == true ? true : allFields.positions;
                }
            }
            // 返回string类型的func
            if (!func.startsWith(FUNC_DISTANCE)) {
                if (termBytes.length == 1) {
                    return new TermPositionsValues(this, readerContext.reader(), field, termBytes[0], allFields);
                } else if (termBytes.length > 1) {
                    TermPositionsValues[] tv = new TermPositionsValues[termBytes.length];
                    for (int i = 0; i < termBytes.length; i++) {
                        tv[i] = new TermPositionsValues(this, readerContext.reader(), field, termBytes[i], allFields);
                    }
                    return new TermsPositionsValues(this, tv);
                } else {
                    return new StrDocValues(this) {
                        @Override
                        public String strVal(int doc) throws IOException {
                            return "";
                        }
                    };
                }
            }
            // 返回int类型的func
            else {
                if (termBytes.length == 1) {
                    return new TermDistanceValues(this, readerContext.reader(), field, termBytes[0], allFields);
                } else if (termBytes.length > 1) {
                    return new TermsDistanceValues(this, readerContext.reader(), field, termBytes, allFields);
                } else {
                    return new IntDocValues(this) {
                        @Override
                        public int intVal(int doc) throws IOException {
                            return DEFAULT_NOT_MATCH;
                        }
                    };
                }
            }
        }
    }


    // term positions ints
    class TermPositionsValues extends StrDocValues {
        final IndexReader reader;
        final String field;
        final BytesRef indexedBytes;
        final FieldOptions fieldOptions;

        int dpEnumFlags = 0;

        public TermPositionsValues(ValueSource vs, IndexReader reader, String field, BytesRef indexedBytes, FieldOptions fieldOptions) throws IOException {
            super(vs);
            this.reader = reader;
            this.field = field;
            this.indexedBytes = indexedBytes;
            this.fieldOptions = fieldOptions;

            this.dpEnumFlags = 0;
            // require position
            this.dpEnumFlags |= fieldOptions.positions ? PostingsEnum.POSITIONS : 0;
            // require offsets
            this.dpEnumFlags |= (fieldOptions.offsets || fieldOptions.payloads) ? PostingsEnum.OFFSETS : 0;
        }


        @Override
        public String strVal(int doc) throws IOException {
            final Terms terms = reader.getTermVector(doc, field);
            if (terms == null) {
                return "";
            }

            TermsEnum termsEnum = terms.iterator();
            if (!termsEnum.seekExact(indexedBytes)) {
                return "";
            }

            final int freq = (int) termsEnum.totalTermFreq();
            PostingsEnum dpEnum = termsEnum.postings(null, this.dpEnumFlags);
            if (dpEnum == null) {
                return "";
            }

            dpEnum.nextDoc();
            String term = indexedBytes.utf8ToString();
            Vector[] termVectors = new Vector[freq];
            for (int i = 0; i < freq; i++) {
                termVectors[i] = new Vector(dpEnum.nextPosition(), fieldOptions.offsets_start ? dpEnum.startOffset() : null, fieldOptions.offsets_end ? dpEnum.endOffset() : null);
            }
            return JSONObject.toJSONString(new TermVector(term, termVectors));
        }
    }

    // terms positions int
    class TermsPositionsValues extends StrDocValues {

        private TermPositionsValues tv[];

        public TermsPositionsValues(ValueSource vs, TermPositionsValues tv[]) {
            super(vs);
            this.tv = tv;
        }

        @Override
        public String strVal(int doc) throws IOException {
            JSONArray jsonArray = new JSONArray();
            for (int i = 0; i < this.tv.length; i++) {
                String val = this.tv[i].strVal(doc);
                if (StringUtils.isNotBlank(val)) {
                    jsonArray.add(JSONObject.parseObject(val));
                }
            }
            return jsonArray.toJSONString();
        }
    }

    // term distance int
    class TermDistanceValues extends IntDocValues {
        final IndexReader reader;
        final String field;
        final BytesRef indexedBytes;
        final FieldOptions fieldOptions;

        int dpEnumFlags = 0;

        public TermDistanceValues(ValueSource vs, IndexReader reader, String field, BytesRef indexedBytes, FieldOptions fieldOptions) throws IOException {
            super(vs);
            this.reader = reader;
            this.field = field;
            this.indexedBytes = indexedBytes;
            this.fieldOptions = fieldOptions;

            this.dpEnumFlags = 0;
            // require position
            this.dpEnumFlags |= fieldOptions.positions ? PostingsEnum.POSITIONS : 0;
            // require offsets
            this.dpEnumFlags |= (fieldOptions.offsets || fieldOptions.payloads) ? PostingsEnum.OFFSETS : 0;
        }

        @Override
        public int intVal(int doc) throws IOException {
            final Terms terms = reader.getTermVector(doc, field);
            if (terms == null) {
                return DEFAULT_NOT_MATCH;
            }

            TermsEnum termsEnum = terms.iterator();
            if (!termsEnum.seekExact(indexedBytes)) {
                return DEFAULT_NOT_MATCH;
            }

            final int freq = (int) termsEnum.totalTermFreq();
            PostingsEnum dpEnum = termsEnum.postings(null, this.dpEnumFlags);
            if (dpEnum == null) {
                return DEFAULT_NOT_MATCH;
            }

            dpEnum.nextDoc();
            int max = -1, min = -1;
            for (int i = 0; i < freq; i++) {
                int position = dpEnum.nextPosition();
                if (fieldOptions.offsets) {
                    int startOffset = dpEnum.startOffset();
                    int endOffset = dpEnum.endOffset();
                    if (fieldOptions.offsets_start) {
                        max = Math.max(max, startOffset);
                        min = min == -1 ? startOffset : Math.min(min, startOffset);
                    } else if (fieldOptions.offsets_end) {
                        max = Math.max(max, endOffset);
                        min = min == -1 ? endOffset : Math.min(min, endOffset);
                    } else {
                        max = Math.max(max, endOffset);
                        min = min == -1 ? startOffset : Math.min(min, startOffset);
                    }
                } else {
                    max = max == -1 ? position : Math.max(max, position);
                    min = min == -1 ? position : Math.min(min, position);
                }
            }
            return max - min;
        }
    }

    // terms distance int
    class TermsDistanceValues extends IntDocValues {
        final IndexReader reader;
        final String field;
        final FieldOptions fieldOptions;
        final BytesRef indexedBytes[];
        int dpEnumFlags = 0;

        public TermsDistanceValues(ValueSource vs, IndexReader reader, String field, BytesRef indexedBytes[], FieldOptions fieldOptions) {
            super(vs);
            this.reader = reader;
            this.field = field;
            this.indexedBytes = indexedBytes;
            this.fieldOptions = fieldOptions;
            this.dpEnumFlags = 0;
            // require position
            this.dpEnumFlags |= fieldOptions.positions ? PostingsEnum.POSITIONS : 0;
            // require offsets
            this.dpEnumFlags |= (fieldOptions.offsets || fieldOptions.payloads) ? PostingsEnum.OFFSETS : 0;
        }

        @Override
        public int intVal(int doc) throws IOException {
            final Terms terms = reader.getTermVector(doc, field);
            if (terms == null) {
                return DEFAULT_NOT_MATCH;
            }
            int max = -1, min = -1;
            for (BytesRef bytesRef : indexedBytes) {
                TermsEnum termsEnum = terms.iterator();
                if (!termsEnum.seekExact(bytesRef)) {
                    continue;
                }
                final int freq = (int) termsEnum.totalTermFreq();
                PostingsEnum dpEnum = termsEnum.postings(null, this.dpEnumFlags);
                if (dpEnum == null) {
                    continue;
                }
                dpEnum.nextDoc();
                for (int i = 0; i < freq; i++) {
                    int position = dpEnum.nextPosition();
                    if (fieldOptions.offsets) {
                        int startOffset = dpEnum.startOffset();
                        int endOffset = dpEnum.endOffset();
                        if (fieldOptions.offsets_start) {
                            max = Math.max(max, startOffset);
                            min = min == -1 ? startOffset : Math.min(min, startOffset);
                        } else if (fieldOptions.offsets_end) {
                            max = Math.max(max, endOffset);
                            min = min == -1 ? endOffset : Math.min(min, endOffset);
                        } else {
                            max = Math.max(max, endOffset);
                            min = min == -1 ? startOffset : Math.min(min, startOffset);
                        }
                    } else {
                        max = max == -1 ? position : Math.max(max, position);
                        min = min == -1 ? position : Math.min(min, position);
                    }
                }
            }
            return max - min;
        }
    }

    private TermInfo parseTerm(FunctionQParser fp) throws SyntaxError, IOException {
        TermInfo tinfo = new TermInfo();

        tinfo.field = fp.parseArg();
        tinfo.val = fp.parseArg();

        // 分词
        ArrayList<BytesRef> termBytes = new ArrayList<>();
        FieldType ft = fp.getReq().getCore().getLatestSchema().getFieldType(tinfo.field);
        Analyzer analyzer = ft.getQueryAnalyzer();
        try (TokenStream in = analyzer.tokenStream(tinfo.field, tinfo.val)) {
            in.reset();
            TermToBytesRefAttribute termAtt = in.getAttribute(TermToBytesRefAttribute.class);
            while (in.incrementToken()) {
                BytesRefBuilder brf = new BytesRefBuilder();
                brf.copyBytes(termAtt.getBytesRef());
                termBytes.add(brf.get());
            }
            in.end();
        }
        tinfo.termBytes = termBytes.toArray(new BytesRef[termBytes.size()]);
        return tinfo;
    }

    public static class TermInfo {
        public String field;
        public String val;
        public BytesRef termBytes[];
    }

    public class FieldOptions {
        String fieldName;
        boolean termFreq, positions, offsets, payloads, offsets_start, offsets_end;
    }

    public class TermVector {
        public String term = "";
        public Vector[] vectors = null;

        public TermVector(String term, Vector[] vectors) {
            this.term = term;
            this.vectors = vectors;
        }

        public String getTerm() {
            return term;
        }

        public void setTerm(String term) {
            this.term = term;
        }

        public Vector[] getVectors() {
            return vectors;
        }

        public void setVectors(Vector[] vectors) {
            this.vectors = vectors;
        }
    }

    public class Vector {
        public Integer position;
        public Integer startOffset;
        public Integer endOffset;

        public Vector(Integer position, Integer startOffset, Integer endOffset) {
            this.position = position;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        public Integer getPosition() {
            return position;
        }

        public void setPosition(Integer position) {
            this.position = position;
        }

        public Integer getStartOffset() {
            return startOffset;
        }

        public void setStartOffset(Integer startOffset) {
            this.startOffset = startOffset;
        }

        public Integer getEndOffset() {
            return endOffset;
        }

        public void setEndOffset(Integer endOffset) {
            this.endOffset = endOffset;
        }
    }

}
