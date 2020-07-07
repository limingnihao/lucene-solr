package org.limingnihao.solr.component;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.*;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.limingnihao.solr.queries.function.EvaluateLinearValueSourceParser;
import org.limingnihao.solr.queries.function.FunctionQueryDebug;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

public class DebugExtendComponent extends SearchComponent {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String COMPONENT_NAME = "DebugExtendComponent";

    public static final String RERANK_DEBUG_NAME = "ReRankDebugInfo";

    public static ScoreMode scoreMode = ScoreMode.COMPLETE;

    @Override
    public void init(NamedList args) {
        super.init(args);
//        log.info("init");
    }

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
//        log.info("prepare");
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
        SolrQueryRequest req = rb.req;
        SolrParams params = req.getParams();
        String ids = params.get(ShardParams.IDS);
        log.info("Debug: {}, Distributed: {}, shards: {}, ids: {}", rb.isDebug(), rb.isDistributed(), rb.shards, ids);
        if (rb.isDebug() && !rb.isDistributed() && ids == null) {
            NamedList debugExtend = new NamedList();
            debugExtend.add("queryDebug", getQueryDebug(rb, req));
            debugExtend.add("rerankDebug", getRerankDebug(rb, req));

            NamedList info = rb.getDebugInfo();
            info.add("debug_extend", debugExtend);
            log.info("{}", debugExtend.toString());
        }
    }

    @Override
    public String getDescription() {
        return "DebugExtendComponent component";
    }

    private NamedList getRerankDebug(ResponseBuilder rb, SolrQueryRequest req) {
        NamedList nl = (NamedList) req.getContext().get(RERANK_DEBUG_NAME);
        return nl;
    }

    private NamedList getQueryDebug(ResponseBuilder rb, SolrQueryRequest req) throws IOException {
        QueryDebugInfo queryDebugInfo = new QueryDebugInfo();
        SolrIndexSearcher searcher = req.getSearcher();
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        Query scoreQuery = searcher.rewrite(rb.getQuery());
        List<Query> filterQuery = rb.getFilters();

        // filterQuery
        if (filterQuery != null && !filterQuery.isEmpty()) {
            log.info("===============filterQuery==============");
            for (Query fq : filterQuery) {
                if (fq instanceof BooleanQuery) {
                    ScoreDebugInfo fqDebugInfo = processBooleanQuery(searcher, leaves, (BooleanQuery) fq);
                    queryDebugInfo.addFQList(fqDebugInfo);
                } else if (fq instanceof TermQuery) {
                    ScoreDebugInfo fqDebugInfo = processTermQuery(searcher, leaves, (TermQuery) fq);
                    queryDebugInfo.addFQList(fqDebugInfo);
                } else {
                    log.warn("还没有解析 query: {}", fq.getClass().getSimpleName());
                }
            }
        }

        // 所有的 scoreQuery
        log.info("++++++++++++++++scoreQuery++++++++++++++++");
        if (scoreQuery instanceof BooleanQuery) {
            ScoreDebugInfo scoreDebugInfo = processBooleanQuery(searcher, leaves, (BooleanQuery) scoreQuery);
            queryDebugInfo.addQList(scoreDebugInfo);
        } else if (scoreQuery instanceof TermQuery) {
            ScoreDebugInfo scoreDebugInfo = processTermQuery(searcher, leaves, (TermQuery) scoreQuery);
            queryDebugInfo.addQList(scoreDebugInfo);
        } else {
            log.warn("还没有解析 query: {}", scoreQuery.getClass().getSimpleName());
        }

        // debug leave
        queryDebugInfo.setLeaveCount(leaves.size());
        for (LeafReaderContext ctx : leaves) {
            // leave详情
            LeaveDebugInfo info = new LeaveDebugInfo(ctx.ord, ctx.docBase, ctx.reader().maxDoc(), ctx.reader().numDocs(), ctx.reader().numDeletedDocs());
            queryDebugInfo.addLeaveInfo(info);
        }
        return queryDebugInfo.asNamedList();
    }

    /**
     * 处理booleanQuery和子句
     */
    protected ScoreDebugInfo processBooleanQuery(SolrIndexSearcher searcher, List<LeafReaderContext> leaves, BooleanQuery query) throws IOException {
        //all query
        ScoreDebugInfo debugAllInfo = this.search(leaves, searcher.createWeight(query, scoreMode, 1));
        debugAllInfo.setType("booleanQuery");
        debugAllInfo.setName(query.toString());

        List<ScoreDebugInfo> debugClauseList = new ArrayList<>();
        for (BooleanClause clause : query.clauses()) {
            Query q = clause.getQuery();
            String occur = clause.getOccur().name();
            ScoreDebugInfo debugClauseInfo = null;
            // boolean
            if (q instanceof BooleanQuery) {
                BooleanQuery bq = (BooleanQuery) clause.getQuery();
                //  有子语句
                if (!((BooleanQuery) q).clauses().isEmpty()) {
                    debugClauseInfo = processBooleanQuery(searcher, leaves, bq);
                }
                // 没有子句了，直接查询
                else {
                    debugClauseInfo = this.search(leaves, searcher.createWeight(bq, scoreMode, 1));
                }
                debugClauseInfo.setType("booleanQuery");
            } else if (q instanceof FunctionScoreQuery) {
                debugClauseInfo = processFunctionScoreQuery(searcher, leaves, (FunctionScoreQuery) q);
            } else if (q instanceof FunctionQuery) {
                debugClauseInfo = processFunctionQuery(searcher, leaves, (FunctionQuery) q);
            } else if (q instanceof TermQuery) {
                debugClauseInfo = processTermQuery(searcher, leaves, (TermQuery) q);
            } else {
                debugClauseInfo = this.search(leaves, searcher.createWeight(q, scoreMode, 1));
                debugClauseInfo.setType(q.getClass().getSimpleName());
                debugClauseInfo.setName(q.toString());
            }
            debugClauseInfo.setType(debugClauseInfo.type + " (" + occur + ")");
            debugClauseList.add(debugClauseInfo);
            debugAllInfo.children = debugClauseList.toArray(new ScoreDebugInfo[debugClauseList.size()]);
        }
        return debugAllInfo;
    }

    /**
     * 处理functionQuery
     * 一般为在query中的_val_魔术语法函数
     * 特殊处理eval函数，分析每个feature的耗时
     */
    protected ScoreDebugInfo processFunctionQuery(SolrIndexSearcher searcher, List<LeafReaderContext> leaves, FunctionQuery query) throws IOException {
        ScoreDebugInfo debugInfo;
        if (query.getValueSource() instanceof EvaluateLinearValueSourceParser.EvaluateValueSource) {
            FunctionQueryDebug fqd = new FunctionQueryDebug(query.getValueSource());
            FunctionQueryDebug.FunctionWeightDebug weight = fqd.createWeight(searcher, scoreMode, 1);
            debugInfo = fqd.search(leaves, weight);
            debugInfo.setType("functionQuery(eval)");
            debugInfo.setName(query.toString());
        } else {
            Weight weight = searcher.createWeight(query, scoreMode, 1);
            debugInfo = this.search(leaves, weight);
            debugInfo.setType("functionQuery");
            debugInfo.setName(query.toString());
        }
        return debugInfo;
    }

    /**
     * 处理functionScoreQuery。
     * 一般是数字类型的多值、区间查询
     * a:[0 TO 100]
     * {!edismax qf='a' v='123'}
     */
    protected ScoreDebugInfo processFunctionScoreQuery(SolrIndexSearcher searcher, List<LeafReaderContext> leaves, FunctionScoreQuery query) throws IOException {
        Weight weight = searcher.createWeight(query, scoreMode, 1);
        ScoreDebugInfo debugInfo = this.search(leaves, weight);
        debugInfo.setType("functionScoreQuery");
        debugInfo.setName(query.toString());
        return debugInfo;
    }

    /**
     * termQuery，最底层的query
     */
    protected ScoreDebugInfo processTermQuery(SolrIndexSearcher searcher, List<LeafReaderContext> leaves, TermQuery query) throws IOException {
        Weight weight = searcher.createWeight(query, scoreMode, 1);
        ScoreDebugInfo debugInfo = this.search(leaves, weight);
        debugInfo.setType("termQuery");
        debugInfo.setName(query.toString());
        return debugInfo;
    }

    /**
     * 模拟召回
     */
    protected ScoreDebugInfo search(List<LeafReaderContext> leaves, Weight weight) throws IOException {
        DebugSimpleCollector collector = new DebugSimpleCollector();
        ScoreDebugInfo debugInfo = new ScoreDebugInfo();
        long current = System.nanoTime();
        for (LeafReaderContext ctx : leaves) { // search each subreader
            final LeafCollector leafCollector;
            try {
                leafCollector = collector.getLeafCollector(ctx);
            } catch (CollectionTerminatedException e) {
                // there is no doc of interest in this reader context
                // continue with the following leaf
                continue;
            }
            BulkScorer scorer = weight.bulkScorer(ctx);
            if (scorer != null) {
                try {
                    scorer.score(leafCollector, ctx.reader().getLiveDocs());
                    debugInfo.incLeaf();
                    debugInfo.incCost(scorer.cost());
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
        }
        debugInfo.incTime(System.nanoTime() - current);
        debugInfo.incTotal(collector.count);
        return debugInfo;
    }

    /**
     * 模拟collector
     */
    public static class DebugSimpleCollector extends SimpleCollector {
        private Scorable scorer;
        private int count = 0;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            float score = this.scorer.score();
            // This collector relies on the fact that scorers produce positive values:
            assert score >= 0; // NOTE: false for NaN
            count++;
        }

        @Override
        public ScoreMode scoreMode() {
            return DebugExtendComponent.scoreMode;
        }

        public int getCount() {
            return count;
        }

        public Scorable getScorer() {
            return scorer;
        }
    }

    /**
     * query debug
     * leave，段信息
     * score，召回信息
     */
    public static class QueryDebugInfo {
        private long leaveCount;
        private List<LeaveDebugInfo> leaveList = new ArrayList<>();
        private List<ScoreDebugInfo> qList = new ArrayList<>();
        private List<ScoreDebugInfo> fqList = new ArrayList<>();

        public void setLeaveCount(long leaveCount) {
            this.leaveCount = leaveCount;
        }

        public void addLeaveInfo(LeaveDebugInfo info) {
            this.leaveList.add(info);
        }

        public void addQList(ScoreDebugInfo info) {
            qList.add(info);
        }

        public void addFQList(ScoreDebugInfo info) {
            fqList.add(info);
        }

        public NamedList asNamedList() {
            NamedList<Object> m = new SimpleOrderedMap<>();
            NamedList<Object> m_leaves = new SimpleOrderedMap<>();
            NamedList<Object> m_q = new SimpleOrderedMap<>();
            NamedList<Object> m_fq = new SimpleOrderedMap<>();

            for (LeaveDebugInfo i : leaveList) {
                m_leaves.add(String.valueOf(i.ord), i.asNamedList());
            }
            for (ScoreDebugInfo i : qList) {
                m_q.add(i.name, i.asNamedList());
            }
            for (ScoreDebugInfo i : fqList) {
                m_fq.add(i.name, i.asNamedList());
            }
            m.add("leaveCount", leaveCount);
            m.add("leaveList", m_leaves);
            m.add("q", m_q);
            m.add("fq", m_fq);
            return m;
        }
    }

    // 段信息
    public static class LeaveDebugInfo {
        private final int ord;
        private final int docBase;
        private final int maxDoc;
        private final int numDocs;
        private final int delDocs;

        public LeaveDebugInfo(int ord, int docBase, int maxDoc, int numDocs, int delDocs) {
            this.ord = ord;
            this.docBase = docBase;
            this.maxDoc = maxDoc;
            this.numDocs = numDocs;
            this.delDocs = delDocs;
        }

        public NamedList<Object> asNamedList() {
            NamedList<Object> m = new SimpleOrderedMap<>();
            m.add("docBase", docBase);
            m.add("maxDoc", maxDoc);
            m.add("numDocs", numDocs);
            m.add("delDocs", delDocs);
            return m;
        }
    }

    // 召回信息
    public static class ScoreDebugInfo {
        String name;
        String type;
        long leaf = 0;
        long cost = 0;
        long time = 0;
        long total = 0;
        ScoreDebugInfo[] children;

        public void incLeaf() {
            this.leaf += 1;
        }

        public void incTime(long i) {
            this.time += i;
        }

        public void incTotal(long i) {
            this.total += i;
        }

        public void incCost(long i) {
            this.cost += i;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setType(String type) {
            this.type = type;
        }

        public void setChildren(ScoreDebugInfo[] children) {
            this.children = children;
        }

        @Override
        public String toString() {
            return "{" +
                    "name=" + name +
                    ", leaf=" + leaf +
                    ", cost=" + cost +
                    ", time=" + time +
                    ", total=" + total +
                    '}';
        }

        public NamedList<Object> asNamedList() {
            NamedList<Object> m = new SimpleOrderedMap<>();
            m.add("type", type);
            m.add("leaf", leaf);
            m.add("cost", cost);
            m.add("time", time / 1000000.0);
            m.add("total", total);
            if (children != null) {
                NamedList<Object> m_children = new SimpleOrderedMap<>();
                for (ScoreDebugInfo i : children) {
                    if (i != null) {
                        m_children.add(i.name, i.asNamedList());
                    }
                }
                m.add("children", m_children);
            }
            return m;
        }
    }

}
