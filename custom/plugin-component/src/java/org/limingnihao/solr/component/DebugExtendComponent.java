package org.limingnihao.solr.component;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
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

    @Override
    public void init(NamedList args) {
        super.init(args);
        log.info("init");
    }

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        log.info("prepare");
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

        QueryDebugInfo queryInfo = new QueryDebugInfo();
        Query query = rb.getQuery();
        SolrIndexSearcher searcher = req.getSearcher();
        DebugSimpleCollector collector = new DebugSimpleCollector();
        Weight weight = searcher.createWeight(query, collector.scoreMode(), 1);
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        queryInfo.setLeaveCount(leaves.size());
        this.search(leaves, weight, collector, queryInfo);
        log.info("collector - count: {}", collector.count);
        return queryInfo.asNamedList();
    }

    protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector, QueryDebugInfo queryInfo) throws IOException {
        // TODO: should we make this
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
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
            queryInfo.addLevelInfo(ctx.ord, ctx.docBase, ctx.reader().maxDoc(), ctx.reader().numDocs(), ctx.reader().numDeletedDocs());
        }
    }


    class DebugSimpleCollector extends SimpleCollector {
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
            return ScoreMode.COMPLETE;
        }
    }

    class QueryDebugInfo {
        private long leaveCount;
        private List<LeaveDebugInfo> leaveList = new ArrayList<>();

        public void setLeaveCount(long leaveCount) {
            this.leaveCount = leaveCount;
        }

        public void addLevelInfo(int ord, int docBase, int maxDoc, int numDocs, int delDocs) {
            this.leaveList.add(new LeaveDebugInfo(ord, docBase, maxDoc, numDocs, delDocs));
        }

        public NamedList asNamedList() {
            NamedList<Object> m = new SimpleOrderedMap<>();
            m.add("leaveCount", leaveCount);

            NamedList<Object> m_leaves = new SimpleOrderedMap<>();
            m.add("leaveList", m_leaves);
            for (LeaveDebugInfo i : leaveList) {
                m_leaves.add(String.valueOf(i.ord), i.asNamedList());
            }
            return m;
        }
    }

    class LeaveDebugInfo {
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

        public NamedList asNamedList() {
            NamedList<Object> m = new SimpleOrderedMap<>();
            m.add("docBase", docBase);
            m.add("maxDoc", maxDoc);
            m.add("numDocs", numDocs);
            m.add("delDocs", delDocs);
            return m;
        }
    }
}
