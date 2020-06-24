package org.limingnihao.solr.rerank;

import org.apache.lucene.search.*;
import org.limingnihao.solr.rerank.feature.Feature;
import org.limingnihao.solr.rerank.feature.FeatureWeight;
import org.limingnihao.solr.rerank.feature.SolrFeature;
import org.limingnihao.solr.rerank.feature.FeatureWeight;
import org.limingnihao.solr.rerank.model.ReRankScoringModel;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.lucene.util.Accountable;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * rerank scoring
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class ReRankScoringQuery extends Query implements Accountable {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // 模型
    private ReRankScoringModel reRankModel;

    // Map of external parameters, such as query intent, that can be used by
    // features
    private Map<String, String[]> params;

    // Original solr query used to fetch matching documents
    private Query originalQuery;

    // Original solr request
    private SolrQueryRequest request;

    // feature logger to output the features.
    private FeatureLogger featureLogger;

    // thread
    private final ReRankThreadModule threadModule;

    // limits the number of threads per query, so that multiple requests can be serviced simultaneously
    private final Semaphore querySemaphore;

    public ReRankScoringQuery(SolrQueryRequest request, ReRankScoringModel reRankModel, Map<String, String[]> params, ReRankThreadModule threadModule) {
        this.request = request;
        this.reRankModel = reRankModel;
        this.params = params;
        this.threadModule = threadModule;
        if (this.threadModule != null) {
            this.querySemaphore = this.threadModule.createQuerySemaphore();
        } else {
            this.querySemaphore = null;
        }
    }

    @Override
    public ReRankWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        List<FeatureWeight> featureWeightList = new ArrayList<>();
        if (this.reRankModel == null || this.reRankModel.getFeatures() == null || this.reRankModel.getFeatures().length == 0) {
            log.error("Error while creating weights in ReRank: InterruptedException. model is null or feature is null");
            throw new RuntimeException("Error while creating weights in ReRank: model is null or feature is null");
        }
        if (this.threadModule != null && this.querySemaphore != null) {
            this.createFeatureWeightsParallel(searcher, scoreMode.needsScores(), featureWeightList, this.reRankModel.getFeatures());
        } else {
            this.createFeatureWeights(searcher, scoreMode.needsScores(), featureWeightList, this.reRankModel.getFeatures());
        }
        int size = featureWeightList != null ? featureWeightList.size() : 0;
        return new ReRankWeight(this, this.reRankModel, featureWeightList.toArray(new FeatureWeight[]{}), size);
    }

    /**
     * 同步创建
     */
    public void createFeatureWeights(IndexSearcher searcher, boolean needsScores, List<FeatureWeight> featureWeightList, Feature[] features) {
        for (int i = 0; i < features.length; i++) {
            try {
                FeatureWeight fw = features[i].createWeight(searcher, needsScores, request, originalQuery, params);
                featureWeightList.add(fw);
            } catch (Exception e) {
                log.error("Error while creating weights in ReRank: InterruptedException. e: {}", ExceptionUtils.getRootCause(e));
                throw new RuntimeException("Error while creating weights in ReRank: " + ExceptionUtils.getRootCause(e), ExceptionUtils.getRootCause(e));
            }
        }
    }

    /**
     * 异步创建
     */
    public void createFeatureWeightsParallel(IndexSearcher searcher, boolean needsScores, List<FeatureWeight> featureWeightList, Feature[] features) {
        List<Future<FeatureWeight>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < features.length; i++) {
                CreateWeightCallable callable = new CreateWeightCallable(searcher, needsScores, request, params, features[i]);
                RunnableFuture<FeatureWeight> runnableFuture = new FutureTask<FeatureWeight>(callable);
                querySemaphore.acquire(); // always acquire before the ltrSemaphore is acquired, to guarantee a that the current query is within the limit for max. threads
                threadModule.acquireLTRSemaphore();//may block and/or interrupt
                threadModule.execute(runnableFuture);//releases semaphore when done
                futures.add(runnableFuture);
            }
            //Loop over futures to get the feature weight objects
            for (final Future<FeatureWeight> future : futures) {
                featureWeightList.add(future.get()); // future.get() will block if the job is still running
            }
        } catch (Exception e) { // To catch InterruptedException and ExecutionException
            log.error("Error while creating weights in ReRank: InterruptedException. e: {}", ExceptionUtils.getRootCause(e));
            throw new RuntimeException("Error while creating weights in ReRank: " + ExceptionUtils.getRootCause(e), ExceptionUtils.getRootCause(e));
        }
    }

    public Query getOriginalQuery() {
        return this.originalQuery;
    }

    public void setOriginalQuery(Query originalQuery) {
        this.originalQuery = originalQuery;
        try {
            this.originalQuery = this.request.getSearcher().rewrite(originalQuery);
        } catch (IOException e) {
            log.error("Error while setOriginalQuery. e: {}", ExceptionUtils.getRootCause(e));
        }
        if (log.isDebugEnabled()) {
            log.debug("setOriginalQuery - rewrite: {}, in: {}", this.originalQuery, originalQuery);
        }
    }

    public void setFeatureLogger(FeatureLogger featureLogger) {
        this.featureLogger = featureLogger;
    }

    public FeatureLogger getFeatureLogger() {
        return this.featureLogger;
    }

    @Override
    public String toString(String field) {
        return field;
    }

    @Override
    public void visit(QueryVisitor visitor) {
    }

    @Override
    public boolean equals(Object o) {
        return sameClassAs(o) && equalsTo(getClass().cast(o));
    }

    private boolean equalsTo(ReRankScoringQuery other) {
        if (reRankModel == null) {
            if (other.reRankModel != null) {
                return false;
            }
        } else if (!reRankModel.equals(other.reRankModel)) {
            return false;
        }
        if (originalQuery == null) {
            if (other.originalQuery != null) {
                return false;
            }
        } else if (!originalQuery.equals(other.originalQuery)) {
            return false;
        }

        if (featureLogger == null) {
            if (other.featureLogger != null) {
                return false;
            }
        } else if (!featureLogger.equals(other.featureLogger)) {
            return false;
        }

        if (params == null) {
            if (other.params != null) {
                return false;
            }
        } else {
            if (other.params == null || params.size() != other.params.size()) {
                return false;
            }
            for (final Map.Entry<String, String[]> entry : params.entrySet()) {
                final String key = entry.getKey();
                final String[] otherValues = other.params.get(key);
                if (otherValues == null || !Arrays.equals(otherValues, entry.getValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = getClass().getName().hashCode();
        result = (prime * result) + ((reRankModel == null) ? 0 : reRankModel.hashCode());
        result = (prime * result) + ((originalQuery == null) ? 0 : originalQuery.hashCode());
        if (params == null) {
            result = (prime * result) + 0;
        } else {
            for (final Map.Entry<String, String[]> entry : params.entrySet()) {
                final String key = entry.getKey();
                final String[] values = entry.getValue();
                result = (prime * result) + key.hashCode();
                result = (prime * result) + Arrays.hashCode(values);
            }
        }
        result = (prime * result) + this.toString().hashCode();
        return result;
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    /**
     * 异步创建weight
     */
    protected class CreateWeightCallable implements Callable<FeatureWeight> {
        final private IndexSearcher searcher;
        final private boolean needsScores;
        final private SolrQueryRequest request;
        final private Map<String, String[]> params;
        final private Feature feature;

        public CreateWeightCallable(IndexSearcher searcher, boolean needsScores, SolrQueryRequest request, Map<String, String[]> params, Feature feature) {
            this.searcher = searcher;
            this.needsScores = needsScores;
            this.request = request;
            this.params = params;
            this.feature = feature;
        }

        @Override
        public FeatureWeight call() throws Exception {
            try {
                FeatureWeight fw = feature.createWeight(searcher, needsScores, request, originalQuery, params);
                return fw;
            } catch (final Exception e) {
                throw new RuntimeException("Exception from createWeight for, error: {}", e);
            } finally {
                querySemaphore.release();
                threadModule.releaseLTRSemaphore();
            }
        }
    } // end of call CreateWeightCallable
}
