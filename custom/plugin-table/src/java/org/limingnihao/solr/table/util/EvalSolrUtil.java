package org.limingnihao.solr.table.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.macro.MacroExpander;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

import java.io.IOException;
import java.util.Map;

public class EvalSolrUtil {

    public static boolean evalBoolean(int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext, String qstr, Map<String, String[]> orig) {
        try {
            FunctionValues fvs = getFunctionValues(req, context, readerContext, qstr, orig);
            return fvs.boolVal(doc);
        } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ExceptionUtils.getMessage(e));
        }
    }

    public static float evalFloat(int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext, String qstr, Map<String, String[]> orig) {
        try {
            FunctionValues fvs = getFunctionValues(req, context, readerContext, qstr, orig);
            return fvs.floatVal(doc);
        } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ExceptionUtils.getMessage(e));
        }
    }

    public static FunctionValues getFunctionValues(SolrQueryRequest req, Map context, LeafReaderContext readerContext, String qstr, Map<String, String[]> orig) {
        try {
            MacroExpander macroExpander = new MacroExpander(orig, true);
            qstr = macroExpander.expand(qstr);
            QParser qParser = FunctionQParser.getParser(qstr, req);
            Query query = qParser.parse();
            ValueSource vs = null;
            if (query == null) {
                vs = new ConstValueSource(0);
            } else if (query instanceof FunctionQuery) {
                vs = ((FunctionQuery) query).getValueSource();
            }
            FunctionValues fvs = vs.getValues(context, readerContext);
            return fvs;
        } catch (SyntaxError | IOException e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ExceptionUtils.getMessage(e));
        }
    }
}
