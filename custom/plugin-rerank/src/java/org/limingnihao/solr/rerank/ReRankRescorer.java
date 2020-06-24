package org.limingnihao.solr.rerank;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RTimerTree;
import org.limingnihao.solr.rerank.feature.FeatureDebug;
import org.limingnihao.solr.rerank.feature.FeatureInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

/**
 * rerank rescorer
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class ReRankRescorer extends QueryRescorer {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ReRankScoringQuery scoringQuery;

    private final double reRankWeight;
    private final double firstMinimum;
    private final float firstWeight;
    private final int timeout;
    private boolean isDebug;
    private boolean isDebugTiming;
    private boolean isDebugRerank;

    public ReRankRescorer(ReRankScoringQuery scoringQuery, double reRankWeight, double firstMinimum, float firstWeight, int timeout) {
        super(scoringQuery);
        this.scoringQuery = scoringQuery;
        this.reRankWeight = reRankWeight;
        this.firstMinimum = firstMinimum;
        this.firstWeight = firstWeight;
        this.timeout = timeout;
        this.isDebug = ReRankContextUtils.isDebug();
        this.isDebugTiming = ReRankContextUtils.isDebugTimings();
        this.isDebugRerank = ReRankContextUtils.isDebugRerank();
    }

    @Override
    protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
        float score = firstPassScore;

        // 是否使用二排分
        if (secondPassMatches && firstPassScore >= firstMinimum) {
            // 一排分权重
            score = firstPassScore * firstWeight;
            // 二排分权重
            score += reRankWeight * secondPassScore;
        }
        return score;
    }

    /**
     * QueryRescorer rescore源代码，只增加一行featureLog，用户保存得分。
     * 获取weight，getFeatureInfo进行保存得分
     */
    @Override
    public TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN) throws IOException {
        if (isDebug) {
            return rescoreDebug(searcher, firstPassTopDocs, topN);
        } else {
            return rescoreCommon(searcher, firstPassTopDocs, topN);
        }
    }

    public TopDocs rescoreDebug(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN) throws IOException {
        ReRankDebugInfo debugInfo = new ReRankDebugInfo();

        long timer_scorer = 0;
        long timer_score = 0;

        ScoreDoc[] hits = firstPassTopDocs.scoreDocs.clone();

        // 用于存储feature
        Map<Integer, FeatureDoc> featureMap = new HashMap<>(hits.length);

        Arrays.sort(hits,
                new Comparator<ScoreDoc>() {
                    @Override
                    public int compare(ScoreDoc a, ScoreDoc b) {
                        return a.doc - b.doc;
                    }
                });

        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

        // debug
        debugInfo.setLeaveCount(leaves.size());
        for (LeafReaderContext ctx : leaves) {
            debugInfo.addLevelInfo(ctx.ord, ctx.docBase, ctx.reader().maxDoc(), ctx.reader().numDocs(), ctx.reader().numDeletedDocs());
        }

        Query rewritten = searcher.rewrite(scoringQuery);
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);

        // Now merge sort docIDs from hits, with reader's leaves:
        int hitUpto = 0;
        int readerUpto = -1;
        int endDoc = 0;
        int docBase = 0;
        Scorer scorer = null;
        float firstScore = 0f;
        float secondScore = 0f;
        while (hitUpto < hits.length) {
            ScoreDoc hit = hits[hitUpto];
            int docID = hit.doc;
            LeafReaderContext readerContext = null;
            while (docID >= endDoc) {
                readerUpto++;
                readerContext = leaves.get(readerUpto);
                endDoc = readerContext.docBase + readerContext.reader().maxDoc();
            }

            // scorer time 统计
            long current = System.nanoTime();

            if (readerContext != null) {
                // We advanced to another segment:
                docBase = readerContext.docBase;
                scorer = weight.scorer(readerContext);
            }

            // score time 统计
            timer_scorer += System.nanoTime() - current;
            current = System.nanoTime();

            if (scorer != null) {
                int targetDoc = docID - docBase;
                int actualDoc = scorer.docID();
                if (actualDoc < targetDoc) {
                    actualDoc = scorer.iterator().advance(targetDoc);
                }
                secondScore = scorer.score();
                firstScore = hit.score;
                if (actualDoc == targetDoc) {
                    // Query did match this doc:
                    hit.score = combine(hit.score, true, secondScore);
                } else {
                    // Query did not match this doc:
                    assert actualDoc > targetDoc;
                    hit.score = combine(hit.score, false, 0.0f);
                }
            } else {
                // Query did not match this doc:
                hit.score = combine(hit.score, false, 0.0f);
            }
            // 保存feature map
            featureMap.put(hit.doc, new FeatureDoc(weight, firstScore, secondScore));
            hitUpto++;

            // score time 统计 end
            timer_score += System.nanoTime() - current;
        }

        Comparator<ScoreDoc> sortDocComparator = new Comparator<ScoreDoc>() {
            @Override
            public int compare(ScoreDoc a, ScoreDoc b) {
                // Sort by score descending, then docID ascending:
                if (a.score > b.score) {
                    return -1;
                } else if (a.score < b.score) {
                    return 1;
                } else {
                    // This subtraction can't overflow int
                    // because docIDs are >= 0:
                    return a.doc - b.doc;
                }
            }
        };

        if (topN < hits.length) {
            ArrayUtil.select(hits, 0, hits.length, topN, sortDocComparator);
            ScoreDoc[] subset = new ScoreDoc[topN];
            System.arraycopy(hits, 0, subset, 0, topN);
            hits = subset;
        }

        Arrays.sort(hits, sortDocComparator);

        // 存储feature, 在二排排序完以后，在进行保存feature，取top N
        if (this.scoringQuery.getFeatureLogger() != null) {
            for (int i = 0; i < hits.length; i++) {
                FeatureDoc featureDoc = featureMap.get(hits[i].doc);
                if (featureDoc != null) {
                    scoreFeatures(searcher, hits[i].doc, featureDoc.featureInfos, featureDoc.firstScore, featureDoc.modelScore, i);
                }
            }
        }

        if (isDebug) {
            if (isDebugTiming) {
                RTimerTree rerankRTimer = ReRankContextUtils.getTimer("process.query.rerank");
                RTimerTree scorerRTimer = new FeatureRTimer(timer_scorer / 1000000);
                RTimerTree scoreRTimer = new FeatureRTimer(timer_score / 1000000);
                rerankRTimer.getChildren().add("scorer", scorerRTimer);
                rerankRTimer.getChildren().add("score", scoreRTimer);
                ReRankContextUtils.stopTimer("process.query.rerank");
            }
            // rerank debug info
            if (isDebugRerank) {
                FeatureDebug[] featureDebugs = ((ReRankWeight) weight).getFeatureDebugs();
                for (FeatureDebug fd : featureDebugs) {
                    log.info("FeatureDebug - name: {}, total: {}, cost: {}, time: {}", fd.getName(), fd.getTotal(), fd.getCost(), fd.getTime() / 1000000);
                }
                debugInfo.setFeatureDebug(featureDebugs);
            }
            ReRankContextUtils.setDebugInfo(debugInfo);
        }
        return new TopDocs(firstPassTopDocs.totalHits, hits);
    }

    public TopDocs rescoreCommon(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN) throws IOException {
        ScoreDoc[] hits = firstPassTopDocs.scoreDocs.clone();

        // 用于存储feature
        Map<Integer, FeatureDoc> featureMap = new HashMap<>(hits.length);

        Arrays.sort(hits,
                new Comparator<ScoreDoc>() {
                    @Override
                    public int compare(ScoreDoc a, ScoreDoc b) {
                        return a.doc - b.doc;
                    }
                });

        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

        Query rewritten = searcher.rewrite(scoringQuery);
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);

        // Now merge sort docIDs from hits, with reader's leaves:
        int hitUpto = 0;
        int readerUpto = -1;
        int endDoc = 0;
        int docBase = 0;
        Scorer scorer = null;
        float firstScore = 0f;
        float secondScore = 0f;
        while (hitUpto < hits.length) {
            ScoreDoc hit = hits[hitUpto];
            int docID = hit.doc;
            LeafReaderContext readerContext = null;
            while (docID >= endDoc) {
                readerUpto++;
                readerContext = leaves.get(readerUpto);
                endDoc = readerContext.docBase + readerContext.reader().maxDoc();
            }
            if (readerContext != null) {
                // We advanced to another segment:
                docBase = readerContext.docBase;
                scorer = weight.scorer(readerContext);
            }
            if (scorer != null) {
                int targetDoc = docID - docBase;
                int actualDoc = scorer.docID();
                if (actualDoc < targetDoc) {
                    actualDoc = scorer.iterator().advance(targetDoc);
                }
                secondScore = scorer.score();
                firstScore = hit.score;
                if (actualDoc == targetDoc) {
                    // Query did match this doc:
                    hit.score = combine(hit.score, true, secondScore);
                } else {
                    // Query did not match this doc:
                    assert actualDoc > targetDoc;
                    hit.score = combine(hit.score, false, 0.0f);
                }
            } else {
                // Query did not match this doc:
                hit.score = combine(hit.score, false, 0.0f);
            }
            // 保存feature map
            featureMap.put(hit.doc, new FeatureDoc(weight, firstScore, secondScore));
            hitUpto++;
        }

        Comparator<ScoreDoc> sortDocComparator = new Comparator<ScoreDoc>() {
            @Override
            public int compare(ScoreDoc a, ScoreDoc b) {
                // Sort by score descending, then docID ascending:
                if (a.score > b.score) {
                    return -1;
                } else if (a.score < b.score) {
                    return 1;
                } else {
                    // This subtraction can't overflow int
                    // because docIDs are >= 0:
                    return a.doc - b.doc;
                }
            }
        };

        if (topN < hits.length) {
            ArrayUtil.select(hits, 0, hits.length, topN, sortDocComparator);
            ScoreDoc[] subset = new ScoreDoc[topN];
            System.arraycopy(hits, 0, subset, 0, topN);
            hits = subset;
        }

        Arrays.sort(hits, sortDocComparator);

        // 存储feature, 在二排排序完以后，在进行保存feature，取top N
        if (this.scoringQuery.getFeatureLogger() != null) {
            for (int i = 0; i < hits.length; i++) {
                FeatureDoc featureDoc = featureMap.get(hits[i].doc);
                if (featureDoc != null) {
                    scoreFeatures(searcher, hits[i].doc, featureDoc.featureInfos, featureDoc.firstScore, featureDoc.modelScore, i);
                }
            }
        }
        return new TopDocs(firstPassTopDocs.totalHits, hits);
    }

    /**
     * 保存 feature
     */
    public void scoreFeatures(IndexSearcher indexSearcher, int docid, FeatureInfo[] featuresInfo, float firstScore, float modelScore, int hitUpto) {
        FeatureLogger featureLogger = this.scoringQuery.getFeatureLogger();
        if (featureLogger != null && indexSearcher instanceof SolrIndexSearcher) {
            featureLogger.log(docid, this.scoringQuery, (SolrIndexSearcher) indexSearcher, featuresInfo, firstScore, modelScore, hitUpto);
        }
    }

    public static FeatureInfo[] extractFeaturesInfo(int docid, List<LeafReaderContext> leafContexts, ReRankWeight modelWeight, Float modelScore) throws IOException {
        final int n = ReaderUtil.subIndex(docid, leafContexts);
        final LeafReaderContext atomicContext = leafContexts.get(n);
        final int deBasedDoc = docid - atomicContext.docBase;
        final ReRankWeight.ReRankScorer r = modelWeight.scorer(atomicContext);
        if ((r == null) || (r.iterator().advance(deBasedDoc) != deBasedDoc)) {
            return new FeatureInfo[0];
        } else {
            modelScore = r.score();
            return modelWeight.getFeatureInfos();
        }
    }

    /**
     * 用于保存feature
     */
    class FeatureDoc {
        private float firstScore;
        private float modelScore;
        private FeatureInfo[] featureInfos;

        public FeatureDoc(Weight weight, float firstScore, float modelScore) {
            this.firstScore = firstScore;
            this.modelScore = modelScore;
            ReRankWeight reRankWeight = (weight instanceof ReRankWeight) ? (ReRankWeight) weight : null;
            if (reRankWeight != null) {
                this.featureInfos = new FeatureInfo[reRankWeight.getFeatureInfos().length];
                for (int i = 0; i < reRankWeight.getFeatureInfos().length; i++) {
                    this.featureInfos[i] = new FeatureInfo(reRankWeight.getFeatureInfos()[i]);
                }
            }
        }
    }

    class FeatureRTimer extends RTimerTree {

        private double time;

        public FeatureRTimer(double time) {
            this.time = time;
        }

        @Override
        public double stop() {
            return time;
        }

        @Override
        public void pause() {
        }

        @Override
        public void resume() {
        }

        @Override
        public double getTime() {
            return time;
        }

        public void setTime(double time) {
            this.time = time;
        }
    }

}