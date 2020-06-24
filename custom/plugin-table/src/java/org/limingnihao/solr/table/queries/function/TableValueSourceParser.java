package org.limingnihao.solr.table.queries.function;

import org.limingnihao.solr.table.model.TableModel;
import org.limingnihao.solr.table.util.TableUtil;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.StrDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

/**
 * table store查询函数
 * table('table_onthot_id')
 *
 * @author shiming.li
 */
public class TableValueSourceParser extends ValueSourceParser {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String name = "table";
    private static final String DEBUG_TAG = "table";

    // 返回结果多值链接字符
    public static final String FLAG_RESULT_JOIN = ",";

    private boolean debug = false;

    @Override
    public void init(NamedList args) {
        super.init(args);
        log.info("init res:{}", args.toString());
    }

    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String modelName = fp.parseArg();
        TableModel tableModel = TableUtil.mm.getModel(modelName);
        if (tableModel == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, name + " cannot find '" + modelName + "'");
        }
        if (tableModel.getVariables() == null || tableModel.getVariables().isEmpty()) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, name + " model: '" + modelName + "' variables is null");
        }
        if (tableModel.getDatas() == null || tableModel.getDatas().isEmpty()) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, name + " model: '" + modelName + "' datas is null");
        }
        List<ValueSource> variableSources = tableModel.getVariableSources(fp.getReq());
        this.setDebug(fp);

        return new TabletValueSource(tableModel, variableSources, fp.getReq());
    }

    private void setDebug(FunctionQParser fp) {
        String[] dbgParams = fp.getParams().getParams(CommonParams.DEBUG);
        debug = false;
        if (dbgParams != null) {
            for (String dbgParam : dbgParams) {
                if (dbgParam.equals(DEBUG_TAG)) {
                    debug = true;
                }
            }
        }
    }

    /**
     * value source
     */
    public class TabletValueSource extends ValueSource {
        private final TableModel tableModel;
        private final List<ValueSource> sources;
        private final SolrQueryRequest req;

        public TabletValueSource(TableModel tableModel, List<ValueSource> sources, SolrQueryRequest req) {
            this.tableModel = tableModel;
            this.sources = sources;
            this.req = req;
        }

        @Override
        public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
            FunctionValues[] valFuncs = new FunctionValues[this.sources.size()];
            for (int i = 0; i < this.sources.size(); i++) {
                valFuncs[i] = this.sources.get(i).getValues(context, readerContext);
            }
            return new TableFunctionValues(this, tableModel, valFuncs, this.req, context, readerContext);
        }

        @Override
        public boolean equals(Object o) {
            if (this.getClass() != o.getClass()) return false;
            TabletValueSource other = (TabletValueSource) o;
            return tableModel.equals(other.tableModel);
        }

        @Override
        public int hashCode() {
            return tableModel.hashCode() + name.hashCode();
        }

        @Override
        public String description() {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append('(');
            sb.append(')');
            return sb.toString();
        }
    }

    class TableFunctionValues extends StrDocValues {
        private final TableModel tableModel;
        private final FunctionValues[] valFuncs;
        private final SolrQueryRequest req;
        private final Map context;
        private final LeafReaderContext readerContext;

        public TableFunctionValues(ValueSource vs, TableModel tableModel, FunctionValues[] valFuncs, SolrQueryRequest req, Map context, LeafReaderContext readerContext) {
            super(vs);
            this.tableModel = tableModel;
            this.valFuncs = valFuncs;
            this.req = req;
            this.context = context;
            this.readerContext = readerContext;
        }

        @Override
        public String strVal(int doc) throws IOException {
            String[] variables = getVariables(doc);
            float[] floatVals = tableModel.floatVals(variables, doc, this.req, this.context, this.readerContext);
            if (floatVals != null) {
                String result = "";
                for (int i = 0; i < floatVals.length; i++) {
                    result += floatVals[i] + FLAG_RESULT_JOIN;
                }
                return result;
            } else {
                return null;
            }
        }

        @Override
        public String toString(int doc) throws IOException {
            return tableModel.toString(getVariables(doc), doc, this.req, this.context, this.readerContext);
        }

        @Override
        public Explanation explain(int doc) throws IOException {
            return tableModel.explain(getVariables(doc), doc, this.req, this.context, this.readerContext);
        }

        public String[] getVariables(int doc) throws IOException {
            String[] variables = new String[valFuncs.length];
            for (int i = 0; i < valFuncs.length; i++) {
                variables[i] = valFuncs[i].strVal(doc);
            }
            return variables;
        }

    }
}
