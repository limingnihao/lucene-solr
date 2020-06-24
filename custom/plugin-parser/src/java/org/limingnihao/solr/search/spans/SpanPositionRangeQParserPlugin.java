package org.limingnihao.solr.search.spans;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanPositionRangeQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;

import java.io.IOException;

/**
 * @author shiming.li
 */
public class SpanPositionRangeQParserPlugin extends QParserPlugin {

    public static final String NAME = "span_p_first";
    public static final String START = "start";
    public static final String END = "end";

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        return new QParser(qstr, localParams, params, req) {
            @Override
            public Query parse() throws SyntaxError {
                String field = localParams.get(QueryParsing.F);
                String value = localParams.get(QueryParsing.V);

                int start = localParams.getInt(START, -1);
                int end = localParams.getInt(END, -1);

                if (field == null) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'f' not specified");
                }

                if (value == null) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "query string missing");
                }

                try {
                    query = createSpanQuery(field, value, start, end);
                } catch (IOException e) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
                }

                if (query == null) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "SpanQuery is null");
                }
                return query;
            }
        };
    }

    public static SpanQuery createSpanQuery(String field, String value, int start, int end) throws IOException {
        SpanQuery spanQuery = new SpanTermQuery(new Term(field, value));
        if (start == -1) {
            return new SpanFirstQuery(spanQuery, end);
        } else {
            return new SpanPositionRangeQuery(spanQuery, start, end);
        }
    }

}
