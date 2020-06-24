package org.limingnihao.solr.queries.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.spans.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SpanPositionSourceParser extends ValueSourceParser {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String name = "span_position";

    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String field = fp.parseArg();
        String value = fp.parseArg();
        String func = fp.parseArg();
        if (StringUtils.isBlank(field)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `field` is empty!");
        }
        if (StringUtils.isBlank(value)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `value` is empty!");
        }
        FieldType ft = fp.getReq().getCore().getLatestSchema().getFieldType(field);
        Analyzer analyzer = ft.getQueryAnalyzer();
        return new SpanPositionValueSource(field, value, func, analyzer);
    }

    public class SpanPositionValueSource extends ValueSource {
        private final String field;
        private final String value;
        private final String func;
        private final Analyzer analyzer;

        public SpanPositionValueSource(String field, String value, String func, Analyzer analyzer) {
            this.field = field;
            this.value = value;
            this.func = func;
            this.analyzer = analyzer;
        }

        @Override
        public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
            List<SpanTermQuery> terms = new ArrayList<>();
            try (TokenStream in = analyzer.tokenStream(field, value)) {
                in.reset();
                TermToBytesRefAttribute termAtt = in.getAttribute(TermToBytesRefAttribute.class);
                while (in.incrementToken()) {
                    terms.add(new SpanTermQuery(new Term(field, termAtt.getBytesRef())));
                }
                in.end();
            }

            SpanPositionQuery query = new SpanPositionQuery(new SpanOrQuery(terms.toArray(new SpanTermQuery[terms.size()])));
            QueryValueSource qvs = new QueryValueSource(query, 0f);
            FunctionValues fv = qvs.getValues(context, readerContext);
            return new SpanPositionFunctionValues(query, fv, func);
        }

        @Override
        public boolean equals(Object o) {
            SpanPositionValueSource other = (SpanPositionValueSource) o;
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

        class SpanPositionQuery extends SpanPositionCheckQuery {

            private int startPosition = -1;
            private int endPosition = -1;

            public SpanPositionQuery(SpanQuery match) {
                super(match);
            }

            @Override
            protected FilterSpans.AcceptStatus acceptPosition(Spans spans) throws IOException {
                if (this.startPosition != -1) {
                    this.startPosition = Math.min(this.startPosition, spans.startPosition());
                } else {
                    this.startPosition = spans.startPosition();
                }
                if (this.endPosition != -1) {
                    this.endPosition = Math.max(this.endPosition, spans.endPosition());
                } else {
                    this.endPosition = spans.endPosition();
                }
                return FilterSpans.AcceptStatus.YES;
            }

            @Override
            public String toString(String field) {
                StringBuilder buffer = new StringBuilder();
                buffer.append("spanFirst(");
                buffer.append(match.toString(field));
                buffer.append(", ");
                buffer.append(")");
                return buffer.toString();
            }

            public int getStart() {
                return this.startPosition;
            }

            public int getEnd() {
                return this.endPosition;
            }
        }

        class SpanPositionFunctionValues extends FunctionValues {

            private final SpanPositionQuery query;
            private final FunctionValues fv;
            private final String func;

            public SpanPositionFunctionValues(SpanPositionQuery query, FunctionValues fv, String func) {
                this.query = query;
                this.fv = fv;
                this.func = func;
            }

            @Override
            public float floatVal(int doc) throws IOException {
                float f = this.fv.floatVal(doc);
                log.info("score: {}, start:{}, end:{}", f, query.getStart(), query.getEnd());
                if (StringUtils.isBlank(func)) {
                    return f;
                } else if ("start".equals(func)) {
                    return query.getStart();
                } else if ("end".equals(func)) {
                    return query.getEnd();
                } else if ("sub".equals(func)) {
                    return query.getEnd() - query.getStart();
                }
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

}
