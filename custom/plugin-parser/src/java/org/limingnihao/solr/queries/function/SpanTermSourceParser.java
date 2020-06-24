package org.limingnihao.solr.queries.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

public class SpanTermSourceParser extends ValueSourceParser {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String name = "span_term";

    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String field = fp.parseArg();
        String value = fp.parseArg();
        if (StringUtils.isBlank(field)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `field` is empty!");
        }
        if (StringUtils.isBlank(value)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `value` is empty!");
        }
        return new SpanTermValueSource(field, value);
    }

    public class SpanTermValueSource extends ValueSource {
        private final String field;
        private final String value;

        public SpanTermValueSource(String field, String value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
            SpanTermQuery query = new SpanTermQuery(new Term(field, value));
            QueryValueSource qvs = new QueryValueSource(query, 0f);
            FunctionValues fv = qvs.getValues(context, readerContext);
            return new SpanTermFunctionValues(query, qvs, fv);
        }

        @Override
        public boolean equals(Object o) {
            SpanTermValueSource other = (SpanTermValueSource) o;
            if (other != null) {
                return other.field.equals(this.field) && other.value.equals(this.value);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return name.hashCode() + field.hashCode() + value.hashCode();
        }

        @Override
        public String description() {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append('(');
            sb.append("'field':").append(field);
            sb.append("'value':").append(value);
            sb.append(')');
            return sb.toString();
        }
    }

    public class SpanTermFunctionValues extends FunctionValues {

        private final SpanTermQuery query;
        private final QueryValueSource qvs;
        private final FunctionValues fv;

        public SpanTermFunctionValues(SpanTermQuery query, QueryValueSource qvs, FunctionValues fv) {
            this.query = query;
            this.qvs = qvs;
            this.fv = fv;
        }

        @Override
        public float floatVal(int doc) throws IOException {
            float f = this.fv.floatVal(doc);
            return f;
        }

        @Override
        public Explanation explain(int doc) throws IOException {
            return this.fv.explain(doc);
        }

        @Override
        public String toString(int doc) throws IOException {
            return this.fv.toString(doc);
        }
    }

}
