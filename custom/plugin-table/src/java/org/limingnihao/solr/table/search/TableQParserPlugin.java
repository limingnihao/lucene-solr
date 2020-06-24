package org.limingnihao.solr.table.search;

import org.limingnihao.solr.table.store.rest.ManagedTableStore;
import org.limingnihao.solr.table.util.TableUtil;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.rest.RestManager;
import org.apache.solr.search.LuceneQParser;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

public class TableQParserPlugin extends QParserPlugin implements ManagedResourceObserver, ResourceLoaderAware {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        return new LuceneQParser(qstr, localParams, params, req);
    }

    @Override
    public void inform(ResourceLoader loader) throws IOException {
        final SolrResourceLoader solrResourceLoader = (SolrResourceLoader) loader;
        ManagedTableStore.registerManagedMulitpleStore(solrResourceLoader, this);
        RestManager.Registry registry = solrResourceLoader.getManagedResourceRegistry();
        log.info("inform registry:{}", registry);
    }

    @Override
    public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res) throws SolrException {
        if (res instanceof ManagedTableStore) {
            TableUtil.mm = (ManagedTableStore) res;
            log.info("onManagedResourceInitialized res:{}", res);
        }
    }
}
