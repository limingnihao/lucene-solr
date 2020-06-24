package org.limingnihao.solr.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 自定义payload score计算，支持term的boost权重
 *
 * @author shiming.li
 * @date 2020-03-20
 */
public class PayloadScoreBoostUtil {

    public static final String CALCULATE = "calculate";
    public static final String CALCULATE_ALIAS = "calc";

    // 计算规则
    public static final String CALCULATE_MUL = "mul";
    public static final String CALCULATE_SUM = "sum";
    public static final String CALCULATE_DIV = "sum";
    public static final String CALCULATE_SUB = "sub";

    /**
     * The generated SpanQuery will be either a SpanTermQuery or an ordered, zero slop SpanNearQuery, depending
     * on how many tokens are emitted.
     * 解析text是否有权重标识，进行解析
     */
    public static SpanQuery createBoostSpanQuery(String field, String value, Analyzer analyzer, String operator) throws IOException {
        // adapted this from QueryBuilder.createSpanQuery (which isn't currently public) and added reset(), end(), and close() calls
        List<SpanQuery> terms = new ArrayList<>();
        try (TokenStream in = analyzer.tokenStream(field, value)) {
            in.reset();

            TermToBytesRefAttribute termAtt = in.getAttribute(TermToBytesRefAttribute.class);
            while (in.incrementToken()) {
                Term term = new Term(field, termAtt.getBytesRef());

                // 解析boost
                String text = term.text();
                float boost = 1.0f;
                if (text.indexOf('^') > 0) {
                    String text_boost[] = text.split("[\\^]");
                    if (text_boost.length > 0) {
                        text = text_boost[0];
                        boost = Float.parseFloat(text_boost[1]);
                        term = new Term(term.field(), text);
                    }
                }
                SpanBoostQuery spanBoostQuery = new SpanBoostQuery(new SpanTermQuery(term), boost);
                terms.add(spanBoostQuery);
            }
            in.end();
        }

        SpanQuery query;
        if (terms.isEmpty()) {
            query = null;
        } else if (terms.size() == 1) {
            query = new SpanOrQuery(terms.toArray(new SpanBoostQuery[terms.size()]));
        } else if (operator != null && operator.equalsIgnoreCase("or")) {
            query = new SpanOrQuery(terms.toArray(new SpanBoostQuery[terms.size()]));
        } else {
            query = new SpanNearQuery(terms.toArray(new SpanBoostQuery[terms.size()]), 0, true);
        }
        return query;
    }

    /**
     * 解析term的boost
     */
    public static Map<Term, Float> getTermMap(SpanWeight innerWeight) {
        SpanQuery[] spanQueries = null;
        Query query = innerWeight.getQuery();
        if (query instanceof SpanOrQuery) {
            spanQueries = ((SpanOrQuery) query).getClauses();
        } else if (query instanceof SpanNearQuery) {
            spanQueries = ((SpanNearQuery) query).getClauses();
        }
        if (spanQueries != null) {
            Map<Term, Float> termBoostMap = new HashMap<>();
            for (SpanQuery spanQuery : spanQueries) {
                if (spanQuery instanceof SpanBoostQuery) {
                    SpanBoostQuery spanBoostQuery = (SpanBoostQuery) spanQuery;
                    SpanTermQuery spanTermQuery = (SpanTermQuery) spanBoostQuery.getQuery();
                    termBoostMap.put(spanTermQuery.getTerm(), spanBoostQuery.getBoost());
                }
            }
            return termBoostMap;
        }
        return null;
    }


    /**
     * 计算payload最终得分。
     * 从termMap中获取term的boost，然后根据calculate计算最终得分
     *
     * @param payloadFactor 原始payload value
     * @param term          匹配的term
     * @param calculate     sum、mul
     * @param termMap       term boost的map
     * @return 计算后的得分
     */
    public static float calculate(float payloadFactor, Term term, String calculate, Map<Term, Float> termMap) {
        // 此处修改 获取term的boost，然后根据规则进行计算
        if (termMap != null && !termMap.isEmpty() && StringUtils.isNotBlank(calculate)) {
            Float termBoost = termMap.get(term);
            if (termBoost != null) {
                // 相加
                if (PayloadScoreBoostUtil.CALCULATE_SUM.equals(calculate)) {
                    payloadFactor += termBoost;
                }
                // 相乘
                else if (PayloadScoreBoostUtil.CALCULATE_MUL.equals(calculate)) {
                    payloadFactor *= termBoost;
                }
            }
        }
        return payloadFactor;
    }
}
