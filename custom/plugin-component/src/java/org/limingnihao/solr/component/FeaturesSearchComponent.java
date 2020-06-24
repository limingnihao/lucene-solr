package org.limingnihao.solr.component;

import com.alibaba.fastjson.JSONObject;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.search.stats.StatsCache;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeaturesSearchComponent extends SearchComponent {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String COMPONENT_NAME = "features";

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
        log.info("process");
    }


    @Override
    public String getDescription() {
        return "features result component";
    }

}
