package org.limingnihao.solr.rerank.search;

import org.limingnihao.solr.rerank.*;
import org.limingnihao.solr.rerank.model.ReRankLTRModel;
import org.limingnihao.solr.rerank.model.ReRankLinearModel;
import org.limingnihao.solr.rerank.model.ReRankScoringModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.*;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 新的rerank，支持feature输出和cache
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class ReRankExtendQParserPlugin extends QParserPlugin {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String NAME = "rerank_extend";

    // params for setting custom external info that features can use, like query
    // intent
    static final String EXTERNAL_FEATURE_INFO = "efi.";

    private static Query defaultQuery = new MatchAllDocsQuery();

    // 函数方式二排公式
    public static final String RERANK_QUERY = "reRankQuery";

    // json方式二排配置
    public static final String RERANK_JSON = "reRankJson";

    // 解析ltr模型
    public static final String RERANK_LTR = "reRankLtr";

    public static final String RERANK_DOCS = "reRankDocs";
    public static final int RERANK_DOCS_DEFAULT = 200;

    public static final String RERANK_WEIGHT = "reRankWeight";
    public static final double RERANK_WEIGHT_DEFAULT = 1.0d;

    public static final String FIRST_WEIGHT = "firstWeight";
    public static final float FIRST_WEIGHT_DEFAULT = 1.0f;

    public static final String FIRST_MINIMUM = "firstMinimum";
    public static final double FIRST_MINIMUM_DEFAULT = 0d;

    public static final String FEATURE_CACHE = "featureCache";
    public static final String FEATURE_NAMES = "featureNames";
    public static final String FEATURE_DOCS = "featureDocs";

    public static final String TIMEOUT = "timeout";
    public static final int TIMEOUT_DEFAULT = 1000;

    private ReRankThreadModule threadManager = null;

    private String fvCacheName;

    @Override
    public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        return new ReRankQParser(query, localParams, params, req);
    }

    @Override
    public void init(NamedList args) {
        super.init(args);
        threadManager = ReRankThreadModule.getInstance(args);
        SolrPluginUtils.invokeSetters(this, args);
    }

    public void setFvCacheName(String fvCacheName) {
        this.fvCacheName = fvCacheName;
    }

    /**
     * rerank parser
     */
    private class ReRankQParser extends QParser {

        public ReRankQParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
            super(query, localParams, params, req);
        }

        public Query parse() throws SyntaxError {

            String reRankQueryString = localParams.get(RERANK_QUERY);

            String reRankJsonString = localParams.get(RERANK_JSON);

            String reRankLtrString = localParams.get(RERANK_LTR);

            // 两种方式，不能同时为空
            if (StringUtils.isBlank(reRankQueryString) && StringUtils.isBlank(reRankJsonString) && StringUtils.isBlank(reRankLtrString)) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, RERANK_QUERY + " or " + RERANK_JSON + ", " + RERANK_LTR + " parameter is mandatory");
            }

            int reRankDocs = localParams.getInt(RERANK_DOCS, RERANK_DOCS_DEFAULT);
            reRankDocs = Math.max(1, reRankDocs);

            double reRankWeight = localParams.getDouble(RERANK_WEIGHT, RERANK_WEIGHT_DEFAULT);

            // 一排设置
            float firstWeight = localParams.getFloat(FIRST_WEIGHT, FIRST_WEIGHT_DEFAULT);
            double firstMinimum = localParams.getDouble(ReRankExtendQParserPlugin.FIRST_MINIMUM, FIRST_MINIMUM_DEFAULT);

            // 超时
            int timeout = localParams.getInt(TIMEOUT, TIMEOUT_DEFAULT);

            // feature  cache
            String featureCache = localParams.get(FEATURE_CACHE);

            // feature names
            String featureNames = localParams.get(FEATURE_NAMES, "*");

            int featureDocs = localParams.getInt(FEATURE_DOCS, 100);

            // model loading
            ReRankScoringModel scoringModel = null;
            try {
                if (StringUtils.isNotEmpty(reRankJsonString)) {
                    scoringModel = new ReRankLinearModel();
                    scoringModel.loading(reRankJsonString);
                } else if (StringUtils.isNotEmpty(reRankQueryString)) {
                    log.info("reRankQuery: {}", reRankQueryString);
                } else if (StringUtils.isNotEmpty(reRankLtrString)) {
                    scoringModel = new ReRankLTRModel();
                    scoringModel.loading(reRankLtrString);
                    log.info("reRankLtr: {}", reRankLtrString);
                }
            } catch (Exception e) {
                log.error("model loading error: {}", e.getMessage());
                throw new RuntimeException("model loading error: " + ExceptionUtils.getRootCause(e), ExceptionUtils.getRootCause(e));
            }

            if (threadManager != null) {
                threadManager.setExecutor(req.getCore().getCoreContainer().getUpdateShardHandler().getUpdateExecutor());
            }

            // debug rerank
            ReRankContextUtils.setIsDebugRerank(req);

            // 是否进行特征抽取
            final boolean extractFeatures = ReRankContextUtils.isExtractingFeatures(req);

            // 创建 query
            Map<String, String[]> featureParams = extracFeatureParams(params);
            featureParams.putAll(extracFeatureParams(localParams));
            final ReRankScoringQuery scoringQuery = new ReRankScoringQuery(req, scoringModel, featureParams, threadManager);

            // query 存储到上下文
            ReRankContextUtils.setScoringQuery(req, scoringQuery);

            //  如果开启feature cache 设置feature logger.
            if (!extractFeatures && "true".equals(featureCache)) {
                FeatureLogger featureLogger = FeatureLogger.createFeatureLogger(fvCacheName);
                featureLogger.setFeatureNames(featureNames);
                featureLogger.setFeatureDocs(featureDocs);
                ReRankContextUtils.setFeatureLogger(req, featureLogger);
                ReRankContextUtils.setIsCachingFeatures(req);
                scoringQuery.setFeatureLogger(featureLogger);
            }
            // transformer查询 设置feature logger.
            if (extractFeatures && scoringQuery.getFeatureLogger() == null) {
                FeatureLogger featureLogger = ReRankContextUtils.getFeatureLogger(req);
                featureLogger.setFeatureNames(featureNames);
                featureLogger.setFeatureDocs(featureDocs);
                scoringQuery.setFeatureLogger(featureLogger);
            }
            return new ReRankExtendQuery(scoringQuery, reRankDocs, reRankWeight, firstMinimum, firstWeight, timeout);
        }
    }

    /**
     * Given a set of local SolrParams, extract all of the f.key=value params into a map
     *
     * @param localParams Local request parameters that might conatin efi params
     * @return Map of efi params, where the key is the name of the efi param, and the
     * value is the value of the efi param
     */
    public static Map<String, String[]> extracFeatureParams(SolrParams localParams) {
        final Map<String, String[]> externalFeatureInfo = new HashMap<>();
        for (final Iterator<String> it = localParams.getParameterNamesIterator(); it
                .hasNext(); ) {
            final String name = it.next();
            if (name.startsWith(EXTERNAL_FEATURE_INFO)) {
                externalFeatureInfo.put(name.substring(EXTERNAL_FEATURE_INFO.length()), new String[]{localParams.get(name)});
            } else if (QueryParsing.OP.equals(name)) {
                externalFeatureInfo.put(name, new String[]{localParams.get(name)});
            }
        }
        return externalFeatureInfo;
    }

    /**
     * A learning to rank Query, will incapsulate a learning to rank model, and delegate to it the rescoring
     * of the documents.
     **/
    public class ReRankExtendQuery extends AbstractReRankQuery {
        private final ReRankScoringQuery scoringQuery;
        private final double reRankWeight;
        private final double firstMinimum;
        private final float firstWeight;
        private final int timeout;

        public ReRankExtendQuery(ReRankScoringQuery scoringQuery, int reRankDocs, double reRankWeight, double firstMinimum, float firstWeight, int timeout) {
            super(defaultQuery, reRankDocs, new ReRankRescorer(scoringQuery, reRankWeight, firstMinimum, firstWeight, timeout));
            this.scoringQuery = scoringQuery;
            this.reRankWeight = reRankWeight;
            this.firstMinimum = firstMinimum;
            this.firstWeight = firstWeight;
            this.timeout = timeout;
        }


        @Override
        public int hashCode() {
            final int prime = 31;
            int result = classHash();
            result = (prime * result) + mainQuery.hashCode();
            result = (prime * result) + scoringQuery.hashCode();
            result = (prime * result) + reRankDocs;
            result = (prime * result) + (int) reRankWeight;
            result = (prime * result) + (int) firstMinimum;
            result = (prime * result) + (int) firstWeight;
            result = (prime * result) + timeout;
            return result;
        }

        @Override
        public boolean equals(Object o) {
            return sameClassAs(o) && equalsTo(getClass().cast(o));
        }

        private boolean equalsTo(ReRankExtendQuery other) {
            return (mainQuery.equals(other.mainQuery)
                    && scoringQuery.equals(other.scoringQuery)
                    && (reRankDocs == other.reRankDocs)
                    && (reRankWeight == other.reRankWeight)
                    && (firstMinimum == other.firstMinimum)
                    && (firstWeight == other.firstWeight)
                    && (timeout == other.timeout)
            );
        }

        @Override
        public RankQuery wrap(Query _mainQuery) {
            this.scoringQuery.setOriginalQuery(_mainQuery);
            super.wrap(this.scoringQuery.getOriginalQuery());
            return this;
        }

        @Override
        public MergeStrategy getMergeStrategy() {
            return new ReRankMergeStrategy();
        }

        @Override
        public TopDocsCollector getTopDocsCollector(int len, QueryCommand cmd, IndexSearcher searcher) throws IOException {
            if (this.boostedPriority == null) {
                SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
                if (info != null) {
                    Map context = info.getReq().getContext();
                    this.boostedPriority = (Set<BytesRef>)context.get(QueryElevationComponent.BOOSTED);
                }
            }

            if (ReRankContextUtils.isDebugTimings()) {
                ReRankContextUtils.startTimer("process.query.first");
            }
            return new ReRankCollector(reRankDocs, len, reRankQueryRescorer, cmd, searcher, boostedPriority) {
                @Override
                public TopDocs topDocs(int start, int howMany) {
                    if (ReRankContextUtils.isDebugTimings()) {
                        ReRankContextUtils.stopTimer("process.query.first");
                        ReRankContextUtils.startTimer("process.query.rerank");
                    }
                    return super.topDocs(start, howMany);
                }
            };
        }

        @Override
        public String toString(String field) {
            return "{!" + NAME
                    + " mainQuery='" + mainQuery.toString()
                    + "' scoringQuery='" + scoringQuery.toString()
                    + "' reRankDocs=" + reRankDocs
                    + " reRankWeight=" + reRankWeight
                    + " firstMinimum=" + firstMinimum
                    + " firstWeight=" + firstWeight
                    + " timeout=" + timeout
                    + "}";
        }

        @Override
        protected Query rewrite(Query rewrittenMainQuery) throws IOException {
            return new ReRankExtendQuery(scoringQuery, reRankDocs, reRankWeight, firstMinimum, firstWeight, timeout)
                    .wrap(rewrittenMainQuery);
        }

    }


}
