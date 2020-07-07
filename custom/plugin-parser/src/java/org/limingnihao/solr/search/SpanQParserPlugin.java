/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.limingnihao.solr.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class SpanQParserPlugin extends QParserPlugin {
    public static final String NAME = "span";
    public static final String SLOP = "slop";
    public static final String INORDER = "inOrder";
    public static final String DEFAULT_OPERATOR = "phrase";
    public static final String EMPTY = "empty";

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        return new QParser(qstr, localParams, params, req) {
            @Override
            public Query parse() throws SyntaxError {
                String field = localParams.get(QueryParsing.F);
                String value = localParams.get(QueryParsing.V);
                int slop = Integer.parseInt(localParams.get(SLOP, "0"));
                boolean inOrder = localParams.getBool(INORDER, true);
                String operator = localParams.get("operator", DEFAULT_OPERATOR);
                boolean splitOnWhitespace = localParams.getBool(QueryParsing.SPLIT_ON_WHITESPACE, SolrQueryParser.DEFAULT_SPLIT_ON_WHITESPACE);
                boolean empty = localParams.getBool(EMPTY, true);

                if (!(operator.equalsIgnoreCase(DEFAULT_OPERATOR) || operator.equalsIgnoreCase("or"))) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Supported operators are : or , phrase");
                }

                if (field == null) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'f' not specified");
                }

                if (value == null) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "query string missing");
                }
                Analyzer analyzer = null;
                if (splitOnWhitespace) {
                    analyzer = new WhitespaceAnalyzer();
                } else {
                    FieldType ft = req.getCore().getLatestSchema().getFieldType(field);
                    analyzer = ft.getQueryAnalyzer();
                }
                SpanQuery query;
                try {
                    query = createSpanQuery(field, value, analyzer, operator, slop, inOrder);
                } catch (IOException e) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
                }

                if (query == null) {
                    if (empty) {
                        return new MatchNoDocsQuery();
                    } else {
                        return new MatchAllDocsQuery();
                    }
                }
                return query;
            }
        };
    }

    public static SpanQuery createSpanQuery(String field, String value, Analyzer analyzer, String operator, int slop, boolean inOrder) throws IOException {
        List<SpanTermQuery> terms = new ArrayList<>();
        try (TokenStream in = analyzer.tokenStream(field, value)) {
            in.reset();

            TermToBytesRefAttribute termAtt = in.getAttribute(TermToBytesRefAttribute.class);
            while (in.incrementToken()) {
                terms.add(new SpanTermQuery(new Term(field, termAtt.getBytesRef())));
            }
            in.end();
        }

        SpanQuery query;
        if (terms.isEmpty()) {
            query = null;
        } else if (terms.size() == 1) {
            query = terms.get(0);
        } else if (operator != null && operator.equalsIgnoreCase("or")) {
            query = new SpanOrQuery(terms.toArray(new SpanTermQuery[terms.size()]));
        } else {
            query = new SpanNearQuery(terms.toArray(new SpanTermQuery[terms.size()]), slop, inOrder);
        }
        return query;
    }
}
