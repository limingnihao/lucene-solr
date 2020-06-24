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
package org.limingnihao.solr.rerank;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.util.RTimerTree;

/**
 * request context util
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class ReRankContextUtils {

    /**
     * key prefix to reduce possibility of clash with other code's key choices
     **/
    private static final String LTR_PREFIX = "rerank.";

    /**
     * key of the feature logger in the request context
     **/
    private static final String FEATURE_LOGGER = LTR_PREFIX + "feature_logger";

    /**
     * key of the scoring query in the request context
     **/
    private static final String SCORING_QUERY = LTR_PREFIX + "scoring_query";

    /**
     * key of the isExtractingFeatures flag in the request context
     **/
    private static final String IS_EXTRACTING_FEATURES = LTR_PREFIX + "isExtractingFeatures";

    /**
     * key of the isCachingFeatures flag in the request context
     **/
    private static final String IS_CACHING_FEATURES = LTR_PREFIX + "isCachingFeatures";

    /**
     * key of the feature vector store name in the request context
     **/
    private static final String STORE = LTR_PREFIX + "store";

    /**
     * key of the debug
     */
    private static final String IS_RERANK_DEBUG = LTR_PREFIX + CommonParams.DEBUG;

    /**
     * feature logger accessors
     **/
    public static void setFeatureLogger(SolrQueryRequest req, FeatureLogger featureLogger) {
        req.getContext().put(FEATURE_LOGGER, featureLogger);
    }

    public static FeatureLogger getFeatureLogger(SolrQueryRequest req) {
        return (FeatureLogger) req.getContext().get(FEATURE_LOGGER);
    }

    /**
     * scoring query accessors
     **/
    public static void setScoringQuery(SolrQueryRequest req, ReRankScoringQuery scoringQuery) {
        req.getContext().put(SCORING_QUERY, scoringQuery);
    }

    public static ReRankScoringQuery getScoringQuery(SolrQueryRequest req) {
        return (ReRankScoringQuery) req.getContext().get(SCORING_QUERY);
    }

    /**
     * isExtractingFeatures flag accessors
     */
    public static void setIsExtractingFeatures(SolrQueryRequest req) {
        req.getContext().put(IS_EXTRACTING_FEATURES, Boolean.TRUE);
    }

    public static void clearIsExtractingFeatures(SolrQueryRequest req) {
        req.getContext().put(IS_EXTRACTING_FEATURES, Boolean.FALSE);
    }

    public static boolean isExtractingFeatures(SolrQueryRequest req) {
        return Boolean.TRUE.equals(req.getContext().get(IS_EXTRACTING_FEATURES));
    }


    /**
     * isCachingFeatures flag accessors
     */
    public static void setIsCachingFeatures(SolrQueryRequest req) {
        req.getContext().put(IS_CACHING_FEATURES, Boolean.TRUE);
    }

    public static void clearIsCachingFeatures(SolrQueryRequest req) {
        req.getContext().put(IS_CACHING_FEATURES, Boolean.FALSE);
    }

    public static boolean isCachingFeatures(SolrQueryRequest req) {
        return Boolean.TRUE.equals(req.getContext().get(IS_CACHING_FEATURES));
    }


    /**
     * set setIsDebugRerank
     */
    public static void setIsDebugRerank(SolrQueryRequest req) {
        String[] params = req.getParams().getParams(IS_RERANK_DEBUG);
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                if (Boolean.TRUE.toString().equals(params[i])) {
                    req.getContext().put(IS_RERANK_DEBUG, Boolean.TRUE);
                    break;
                }
            }
        }
    }

    /**
     * get isDebugRerank
     */
    public static boolean isDebugRerank() {
        SolrQueryRequest req = SolrRequestInfo.getRequestInfo().getReq();
        return Boolean.TRUE.equals(req.getContext().get(IS_RERANK_DEBUG));
    }

    /**
     * isDebugTimings
     */
    public static boolean isDebugTimings() {
        return SolrRequestInfo.getRequestInfo().getResponseBuilder().isDebugTimings();
    }

    /**
     * isDebug
     */
    public static boolean isDebug() {
        return SolrRequestInfo.getRequestInfo().getResponseBuilder().isDebug();
    }

    /**
     * set debug info
     */
    public static void setDebugInfo(ReRankDebugInfo debugInfo) {
        SolrRequestInfo.getRequestInfo().getReq().getContext().put(ReRankDebugInfo.NAME, debugInfo.asNamedList());
    }

    /**
     * req timer start
     *
     * @path The path of the timer
     */
    public static RTimerTree getTimer(String path) {
        SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        if (info == null) {
            return null;
        }
        SolrQueryRequest req = info.getReq();
        if (req == null) {
            return null;
        }
        RTimerTree parentTimer = req.getRequestTimer();
        if (parentTimer == null) {
            return null;
        }
        if (parentTimer.getChildren() == null) {
            return null;
        }
        String paths[] = path.split("[\\.]");
        int i = 0;
        RTimerTree timer = parentTimer;
        while (timer != null && i < paths.length) {
            timer = timer.getChildren().get(paths[i++]);
        }
        return timer;
    }

    /**
     * req timer start
     *
     * @path The path of the timer
     */
    public static RTimerTree startTimer(String path) {
        SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        if (info == null) {
            return null;
        }
        SolrQueryRequest req = info.getReq();
        if (req == null) {
            return null;
        }
        RTimerTree parentTimer = req.getRequestTimer();
        if (parentTimer == null) {
            return null;
        }
        if (parentTimer.getChildren() == null) {
            return null;
        }
        String paths[] = path.split("[\\.]");
        int i = 0;
        RTimerTree timer = parentTimer;
        while (timer != null && i < paths.length - 1) {
            timer = timer.getChildren().get(paths[i++]);
        }
        if (timer != null) {
            return timer.sub(paths[i]);
        }
        return null;
    }

    /**
     * req timer stop
     *
     * @path The path of the timer
     */
    public static void stopTimer(String path) {
        SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        if (info == null) {
            return;
        }
        SolrQueryRequest req = info.getReq();
        if (req == null) {
            return;
        }
        RTimerTree parentTimer = req.getRequestTimer();
        if (parentTimer == null) {
            return;
        }
        if (parentTimer.getChildren() == null) {
            return;
        }
        String paths[] = path.split("[\\.]");
        int i = 0;
        RTimerTree timer = parentTimer;
        while (timer != null && i < paths.length) {
            timer = timer.getChildren().get(paths[i++]);
        }
        if (timer != null) {
            timer.stop();
        }
    }

}

