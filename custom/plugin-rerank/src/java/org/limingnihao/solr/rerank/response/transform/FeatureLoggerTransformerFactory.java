package org.limingnihao.solr.rerank.response.transform;

import org.apache.lucene.search.ScoreMode;
import org.limingnihao.solr.rerank.*;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

/**
 * feature transformer
 * <p>
 * 需要在solrconfig.xml添加配置
 * transformer name="features_rerank" class="org.limingnihao.solr.rerank.response.transform.FeatureLoggerTransformerFactory"
 * <p>
 * cache配置
 * cache name="features_rerank_cache"
 * class="solr.CaffeineCache"
 * size="4096"
 * initialSize="2048"
 * autowarmCount="4096"
 * regenerator="solr.search.NoOpRegenerator
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class FeatureLoggerTransformerFactory extends TransformerFactory {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private String fvCacheName;

    @Override
    public void init(NamedList args) {
        super.init(args);
        SolrPluginUtils.invokeSetters(this, args);
    }

    @Override
    public DocTransformer create(String name, SolrParams localparams, SolrQueryRequest req) {
        // Hint to enable feature vector cache since we are requesting features
        ReRankContextUtils.setIsExtractingFeatures(req);

        // Create and supply the feature logger to be used
        if (ReRankContextUtils.getFeatureLogger(req) == null) {
            ReRankContextUtils.setFeatureLogger(req, FeatureLogger.createFeatureLogger(this.fvCacheName));
        }

        return new FeatureTransformer(name, localparams, req);
    }

    public void setFvCacheName(String fvCacheName) {
        this.fvCacheName = fvCacheName;
    }

    public class FeatureTransformer extends DocTransformer {

        final private String name;
        final private SolrParams localparams;
        final private SolrQueryRequest req;

        private List<LeafReaderContext> leafContexts;
        private SolrIndexSearcher searcher;
        private ReRankScoringQuery scoringQuery;
        private ReRankWeight modelWeight;
        private FeatureLogger featureLogger;

        // 输出格式
        private String format;

        // 输出的feature name
        private String names;


        /**
         * @param name Name of the field to be added in a document representing the
         *             feature vectors
         */
        public FeatureTransformer(String name, SolrParams localparams, SolrQueryRequest req) {
            this.name = name;
            this.localparams = localparams;
            this.req = req;
            this.format = localparams.get("format");
            this.names = localparams.get("names");
        }

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public void setContext(ResultContext context) {
            super.setContext(context);
            if (context == null) {
                return;
            }
            if (context.getRequest() == null) {
                return;
            }
            searcher = context.getSearcher();
            if (searcher == null) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "searcher is null");
            }
            leafContexts = searcher.getTopReaderContext().leaves();

            scoringQuery = ReRankContextUtils.getScoringQuery(req);
            if (scoringQuery.getOriginalQuery() == null) {
                scoringQuery.setOriginalQuery(context.getQuery());
            }
            featureLogger = scoringQuery.getFeatureLogger();
        }

        @Override
        public void transform(SolrDocument doc, int docid) throws IOException {
            implTransform(doc, docid, null);
        }

        @Override
        public void transform(SolrDocument doc, int docid, float score) throws IOException {
            implTransform(doc, docid, score);
        }

        private void implTransform(SolrDocument doc, int docid, Float score) throws IOException {
            Object fv = featureLogger.getFeatureVector(docid, scoringQuery, searcher);
            if (fv == null || "".equals(fv)) { // FV for this document was not in the cache
                if (modelWeight == null) {
                    modelWeight = scoringQuery.createWeight(searcher, ScoreMode.COMPLETE, 1f);
                }
                try {
                    Float firstScore = null;
                    Float modelScore = null;
                    fv = featureLogger.logAndGet(docid, scoringQuery, searcher,
                            ReRankRescorer.extractFeaturesInfo(
                                    docid,
                                    leafContexts,
                                    modelWeight,
                                    modelScore
                            ),
                            firstScore,
                            modelScore);
                } catch (final Exception e) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.getMessage(), e);
                }
            }
            doc.addField(name, fv);
        }
    }

}
