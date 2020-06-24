package org.limingnihao.solr.table.model;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.macro.MacroExpander;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

import java.util.*;

/**
 * 自定义map store
 *
 * @author shiming.li
 * @date 2020-03-23
 */
public abstract class TableModel implements Accountable {

    private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(TableModel.class);


    // input 拆分字符
    public static final String FLAG_INPUT_SPLIT = "[,; ]";

    // 组装key的链接字符
    public static final String FLAG_KEY_JOIN = "_";

    protected final static String KEY_DEF = "def";
    protected final static String KEY_SIZE = "size";
    protected final static String KEY_VARIABLES = "variables";
    protected final static String KEY_TABLES = "tables";

    protected String name;
    protected Map<String, Object> datas;

    // 解析 datas内部数据
    protected float def;
    protected int size;
    protected LinkedHashMap<String, String> variables;
    protected String[] variableNames;
    protected String[] variableValues;

    public static TableModel getInstance(SolrResourceLoader solrResourceLoader, String className, String name, Map<String, Object> datas) {
        final TableModel model;
        try {
            model = solrResourceLoader.newInstance(
                    className,
                    TableModel.class,
                    new String[0], // no sub packages
                    new Class[]{String.class, Map.class},
                    new Object[]{name, datas});
        } catch (final Exception e) {
            throw new TableException("Model type does not exist " + className, e);
        }
        model.validate();
        return model;
    }

    public TableModel(String name, Map<String, Object> datas) {
        this.name = name;
        this.datas = datas;
        if (datas != null) {
            try {
                if (datas.containsKey(KEY_DEF)) {
                    this.def = Float.parseFloat(datas.get(KEY_DEF).toString());
                }
                if (datas.containsKey(KEY_SIZE)) {
                    this.size = Integer.parseInt(datas.get(KEY_SIZE).toString());
                }
                if (datas.containsKey(KEY_VARIABLES)) {
                    this.variables = (LinkedHashMap<String, String>) datas.get(KEY_VARIABLES);
                    this.variableNames = this.variables.keySet().toArray(new String[this.variables.size()]);
                    this.variableValues = new String[this.variables.size()];
                }
                if (datas.containsKey(KEY_TABLES)) {
                    List<Map> tableList = (List<Map>) datas.get(KEY_TABLES);
                    if (tableList != null && !tableList.isEmpty()) {
                        this.setTables(tableList);
                    }
                }
            } catch (final Exception e) {
                throw new TableException("Model datas is error. " + name, e);
            }
        }
    }

    public abstract void setTables(List<Map> tableList);

    public abstract float[] floatVals(String[] variables);

    public float[] floatVals(String[] variables, int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext) {
        return floatVals(variables);
    }

    public abstract Explanation explain(String[] variables, int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext);

    public abstract String toString(String[] variables, int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext);

    public boolean validate() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableModel that = (TableModel) o;
        return size == that.size &&
                name.equals(that.name) &&
                datas.equals(that.datas) &&
                def == that.def &&
                variables.equals(that.variables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, datas, def, size, variables);
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public String toString() {
        return "Multiple{" +
                "name='" + name + '\'' +
                ", data=" + datas +
                ", def='" + def + '\'' +
                ", size=" + size +
                '}';
    }

    static final String EXTERNAL_FEATURE_INFO = "efi.";

    /**
     * 获取 variables的valuesource
     */
    public List<ValueSource> getVariableSources(SolrQueryRequest req) throws SyntaxError {
        List<ValueSource> variableSources = new ArrayList<>();
        for (String query : getVariables().values()) {
            variableSources.add(getValueSource(query, req));
        }
        return variableSources;
    }

    /**
     * 根据 solrQuery 获取valueSource
     */
    public ValueSource getValueSource(String solrQuery, SolrQueryRequest req) throws SyntaxError {
        if (StringUtils.isEmpty(solrQuery)) {
            throw new IllegalStateException("the param of `ValueSourceFeature` is empty!");
        }
        if (req.getOriginalParams() instanceof MultiMapSolrParams) {
            Map map = extracFeatureParams(req.getOriginalParams());
            MacroExpander macroExpander = new MacroExpander(map, true);
            solrQuery = macroExpander.expand(solrQuery);
        }
        QParser reRankParser = QParser.getParser(solrQuery, req);
        Query query = reRankParser.parse();
        if (query == null) {
            return new ConstValueSource(0);
        } else if (query instanceof FunctionQuery) {
            return ((FunctionQuery) query).getValueSource();
        }
        return new QueryValueSource(query, 0);
    }

    public Map<String, String[]> extracFeatureParams(SolrParams localParams) {
        final Map<String, String[]> externalFeatureInfo = new HashMap<>();
        for (final Iterator<String> it = localParams.getParameterNamesIterator(); it.hasNext(); ) {
            final String name = it.next();
            if (name.startsWith(EXTERNAL_FEATURE_INFO)) {
                externalFeatureInfo.put(name.substring(EXTERNAL_FEATURE_INFO.length()), new String[]{localParams.get(name)});
            } else {
                externalFeatureInfo.put(name, new String[]{localParams.get(name)});
            }
        }
        return externalFeatureInfo;
    }

    public static long getBaseRamBytes() {
        return BASE_RAM_BYTES;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getDatas() {
        return datas;
    }

    public void setDatas(Map<String, Object> datas) {
        this.datas = datas;
    }

    public float getDef() {
        return def;
    }

    public void setDef(float def) {
        this.def = def;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public LinkedHashMap<String, String> getVariables() {
        return variables;
    }

    public void setVariables(LinkedHashMap<String, String> variables) {
        this.variables = variables;
    }

}
