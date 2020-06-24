package org.limingnihao.solr.table.store.rest;

import org.limingnihao.solr.table.model.TableModel;
import org.limingnihao.solr.table.store.TableStore;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.rest.ManagedResourceStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * table store
 * 用于生成一组value的定义
 *
 * @author shiming.li
 * @date 2020-04-28
 */
public class ManagedTableStore extends ManagedResource {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void registerManagedMulitpleStore(SolrResourceLoader solrResourceLoader, ManagedResourceObserver managedResourceObserver) {
        solrResourceLoader.getManagedResourceRegistry().registerManagedResource(
                REST_END_POINT,
                ManagedTableStore.class,
                managedResourceObserver);
    }

    /**
     * the table store rest endpoint
     **/
    private static final String REST_END_POINT = "/schema/table-store";

    /**
     * name of the attribute containing the table class
     **/
    private static final String CLASS_KEY = "class";

    /**
     * name of the attribute containing the table name
     **/
    private static final String NAME_KEY = "name";

    /**
     * name of the attribute containing the table params
     **/
    private static final String DATAS_KEY = "datas";

    private final TableStore store;

    /**
     * Managed table store: the name of the attribute containing all the
     * tables of a table store
     **/
    private static final String JSON_FIELD = "tables";
    
    /**
     * Initializes this managed resource, including setting up JSON-based storage using
     * the provided storageIO implementation, such as ZK.
     *
     * @param resourceId
     * @param loader
     * @param storageIO
     */
    public ManagedTableStore(String resourceId, SolrResourceLoader loader, ManagedResourceStorage.StorageIO storageIO) throws SolrException {
        super(resourceId, loader, storageIO);
        this.store = new TableStore();
    }

    @Override
    public void onManagedDataLoadedFromStorage(NamedList<?> managedInitArgs, Object managedData) throws SolrException {
        log.info("onManagedDataLoadedFromStorage ------ managed table store ~ loading ------ ");
        this.applyUpdatesToManagedData(managedData);
    }

    @Override
    public Object applyUpdatesToManagedData(Object updates) {
        if (updates instanceof List) {
            final List<Map<String, Object>> up = (List<Map<String, Object>>) updates;
            for (final Map<String, Object> u : up) {
                addModelFromMap(u);
            }
        }

        if (updates instanceof Map) {
            // a unique table
            Map<String, Object> updatesMap = (Map<String, Object>) updates;
            addModelFromMap(updatesMap);
        }
        return modelsAsManagedResources(store.getModels());
    }

    public synchronized void addModelFromMap(Map<String, Object> modelMap) {
        TableModel multiple = TableModel.getInstance(solrResourceLoader, (String) modelMap.get(CLASS_KEY), (String) modelMap.get(NAME_KEY), (Map<String, Object>) modelMap.get(DATAS_KEY));
        this.store.addModel(multiple);
    }

    @Override
    public synchronized void doDeleteChild(BaseSolrResource endpoint, String childId) {
        log.info("doDeleteChild, {}, {}", endpoint, childId);
    }

    @Override
    public void doGet(BaseSolrResource endpoint, String childId) {
        final SolrQueryResponse response = endpoint.getSolrResponse();
        response.add(JSON_FIELD, modelsAsManagedResources(store.getModels()));
    }

    public TableModel getModel(String modelName) {
        return store.getModel(modelName);
    }

    @Override
    public String toString() {
        return "ManagedTableStore [store=" + store + "]";
    }


    /**
     * Returns the available models as a list of Maps objects. After an update the
     * managed resources needs to return the resources in this format in order to
     * store in json somewhere (zookeeper, disk...)
     *
     * @return the available models as a list of Maps objects
     */
    private static List<Object> modelsAsManagedResources(List<TableModel> models) {
        final List<Object> list = new ArrayList<>(models.size());
        for (final TableModel model : models) {
            list.add(toModelMap(model));
        }
        return list;
    }

    private static LinkedHashMap<String, Object> toModelMap(TableModel model) {
        final LinkedHashMap<String, Object> modelMap = new LinkedHashMap<>(5, 1.0f);
        modelMap.put(NAME_KEY, model.getName());
        modelMap.put(CLASS_KEY, model.getClass().getName());
        modelMap.put(DATAS_KEY, model.getDatas());
        return modelMap;
    }

}
