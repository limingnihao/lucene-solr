package org.limingnihao.solr.ltr.search;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.search.LTRQParserPlugin;
import org.apache.solr.ltr.store.rest.ManagedFeatureStore;
import org.apache.solr.ltr.store.rest.ManagedModelStore;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.rest.RestManager;
import org.limingnihao.solr.ltr.util.LtrUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;


/**
 * ltr的扩展函数
 * 原始ltr功能，增加加载自定义资源
 *
 * @author shiming.li
 * @date 2020-03-23
 */
public class LtrExtendQParserPlugin extends LTRQParserPlugin implements ManagedResourceObserver {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ManagedFeatureStore fr = null;
    private ManagedModelStore mr = null;

    @Override
    public void inform(ResourceLoader loader) throws IOException {
        final SolrResourceLoader solrResourceLoader = (SolrResourceLoader) loader;
        ManagedFeatureStore.registerManagedFeatureStore(solrResourceLoader, LtrExtendQParserPlugin.this);
        ManagedModelStore.registerManagedModelStore(solrResourceLoader, LtrExtendQParserPlugin.this);
        RestManager.Registry registry = solrResourceLoader.getManagedResourceRegistry();
        log.info("inform registry:{}", registry);
    }

    @Override
    public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res) throws SolrException {
        if (res instanceof ManagedFeatureStore) {
            fr = (ManagedFeatureStore) res;
        }
        if (res instanceof ManagedModelStore) {
            mr = (ManagedModelStore) res;
        }
        if (mr != null && fr != null) {
            mr.setManagedFeatureStore(fr);
            // now we can safely load the models
            mr.loadStoredModels();
        }
        // super.mr = mr;
        // super.fr = fr;
        LtrUtil.fr = fr;
        LtrUtil.mr = mr;
        log.info("*********************************** onManagedResourceInitialized, args: {}, res: {}", args, res.getClass().getName());
    }
}
