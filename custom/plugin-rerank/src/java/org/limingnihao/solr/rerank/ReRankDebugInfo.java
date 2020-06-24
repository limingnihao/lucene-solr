package org.limingnihao.solr.rerank;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.limingnihao.solr.rerank.feature.FeatureDebug;

import java.util.*;

public class ReRankDebugInfo {

    public static String NAME = "ReRankDebugInfo";

    private long leaveCount;
    private List<LeaveInfo> leaveList = new ArrayList<>();
    private List<FeatureDebug> featureList = new ArrayList<>();


    public ReRankDebugInfo() {
    }

    public NamedList asNamedList() {
        NamedList<Object> m = new SimpleOrderedMap<>();
        m.add("leaveCount", leaveCount);

        NamedList<Object> m_leaves = new SimpleOrderedMap<>();
        m.add("leaveList", m_leaves);
        for (LeaveInfo i : leaveList) {
            m_leaves.add(String.valueOf(i.ord), i.asNamedList());
        }

        // feature
        NamedList<Object> m_features = new SimpleOrderedMap<>();
        m.add("features", m_features);
        for (FeatureDebug f : featureList) {
            m_features.add(f.getName(), f.asNamedList());
        }
        return m;
    }

    public void setFeatureDebug(FeatureDebug[] fds) {
        for (FeatureDebug fd : fds) {
            this.featureList.add(fd);
        }
    }

    public void setLeaveCount(long leaveCount) {
        this.leaveCount = leaveCount;
    }

    public void addLevelInfo(int ord, int docBase, int maxDoc, int numDocs, int delDocs) {
        this.leaveList.add(new LeaveInfo(ord, docBase, maxDoc, numDocs, delDocs));
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
