package org.limingnihao.solr.table.util;

import com.udojava.evalex.Expression;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.solr.common.SolrException;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * evalex 开源项目: https://github.com/uklimaschewski/EvalEx
 */
public class EvalExUtil {
    private final static HashMap<String, Expression> DATA_EVALEX = new HashMap<>(100);

    /**
     * 计算表达式的值
     * evalex
     */
    public static float eval(String expression, Map<String, Float> params) {
        Expression e = null;
        if (DATA_EVALEX.containsKey(expression)) {
            e = DATA_EVALEX.get(expression);
        } else {
            e = new Expression(expression);
            DATA_EVALEX.put(expression, e);
        }
        for (Map.Entry<String, Float> entry : params.entrySet()) {
            e.with(entry.getKey(), new BigDecimal(entry.getValue()));
        }
        try {
            return e.eval().floatValue();
        } catch (Exception ex) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ExceptionUtils.getMessage(ex));
        }
    }

    /**
     * 计算表达式的值
     * evalex
     */
    public static float eval(String expression, String[] keys, float[] values) {
        Expression e = null;
        if (DATA_EVALEX.containsKey(expression)) {
            e = DATA_EVALEX.get(expression);
        } else {
            e = new Expression(expression);
            DATA_EVALEX.put(expression, e);
        }
        for (int i = 0; i < keys.length; i++) {
            e.with(keys[i], new BigDecimal(values[i]));
        }
        try {
            return e.eval().floatValue();
        } catch (Exception ex) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ExceptionUtils.getMessage(ex));
        }
    }
}
