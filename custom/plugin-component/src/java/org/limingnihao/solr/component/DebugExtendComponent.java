package org.limingnihao.solr.component;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
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

    private NamedList getQueryDebug(ResponseBuilder rb, SolrQueryRequest req) {
        try {
            QueryDebugInfo queryInfo = new QueryDebugInfo();
            // 一排
            Weight weight = rb.getQuery().createWeight(req.getSearcher(), ScoreMode.COMPLETE, 1f);
            List<LeafReaderContext> leaves = req.getSearcher().getIndexReader().leaves();
            queryInfo.setLeaveCount(leaves.size());
            for (int i = 0; i < leaves.size(); i++) {
                LeafReaderContext ctx = leaves.get(i);
                BulkScorer scorer = weight.bulkScorer(ctx);
                queryInfo.addLevelInfo(ctx.ord, ctx.docBase, ctx.reader().maxDoc(), ctx.reader().numDocs(), ctx.reader().numDeletedDocs());
            }
            return queryInfo.asNamedList();
        } catch (Exception e) {
        }
        return null;
    }

    private NamedList getRerankDebug(ResponseBuilder rb, SolrQueryRequest req) {
        NamedList nl = (NamedList) req.getContext().get(RERANK_DEBUG_NAME);
        return nl;
    }

    class QueryDebugInfo {
        private long leaveCount;
        private List<LeaveInfo> leaveList = new ArrayList<>();

        public void setLeaveCount(long leaveCount) {
            this.leaveCount = leaveCount;
        }

        public void addLevelInfo(int ord, int docBase, int maxDoc, int numDocs, int delDocs) {
            this.leaveList.add(new LeaveInfo(ord, docBase, maxDoc, numDocs, delDocs));
        }

        public NamedList asNamedList() {
            NamedList<Object> m = new SimpleOrderedMap<>();
            m.add("leaveCount", leaveCount);

            NamedList<Object> m_leaves = new SimpleOrderedMap<>();
            m.add("leaveList", m_leaves);
            for (LeaveInfo i : leaveList) {
                m_leaves.add(String.valueOf(i.ord), i.asNamedList());
            }
            return m;
        }
    }

    class LeaveInfo {
        private final int ord;
        private final int docBase;
        private final int maxDoc;
        private final int numDocs;
        private final int delDocs;

        public LeaveInfo(int ord, int docBase, int maxDoc, int numDocs, int delDocs) {
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
