package org.limingnihao.solr.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.solr.common.SolrException;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * evalex 开源项目: https://github.com/uklimaschewski/EvalEx
 * <p>
 * exp4j 开源项目: https://github.com/fasseg/exp4j
 */
public class EvaluateUtil {

    private final static HashMap<String, com.udojava.evalex.Expression> DATA_EVALEX = new HashMap<>(100);

    private final static HashMap<String, net.objecthunter.exp4j.Expression> DATA_EXP4J = new HashMap<>(100);

    private static final Map<String, com.greenpineyu.fel.Expression> DATA_FEL = new HashMap<>(100);

    private static final com.greenpineyu.fel.FelEngine FEL_ENGINE = com.greenpineyu.fel.FelEngine.instance;

    /**
     * 计算表达式的值
     *
     * @param expression 表达式
     * @param keys       变量
     * @param values     值
     * @return 得值
     */
    public static float eval(String expression, String[] keys, float[] values) {
        if (expression.indexOf("sum") == 0) {
            return sum(values);
        } else if (expression.indexOf("exp4j") == 0) {
            return exp4j(expression.substring(6), keys, values);
        } else if (expression.indexOf("evalex") == 0) {
            return evalex(expression.substring(7), keys, values);
        } else if (expression.indexOf("fel") == 0) {
            return fel(expression.substring(4), keys, values);
        } else {
            return exp4j(expression, keys, values);
        }
    }

    /**
     * 计算表达式的值 单纯sum
     *
     * @param values 值
     * @return 得值
     */
    public static float sum(float[] values) {
        float score = 0f;
        for (int i = 0; i < values.length; i++) {
            score += values[i];
        }
        return score;
    }

    /**
     * 计算表达式的值
     * fel库
     */
    public static float fel(String expression, String[] keys, float[] values) {
        com.greenpineyu.fel.Expression e = null;
        if (DATA_FEL.containsKey(expression)) {
            e = DATA_FEL.get(expression);
        } else {
            com.greenpineyu.fel.context.ArrayCtxImpl felCtx = new com.greenpineyu.fel.context.ArrayCtxImpl();
            for (int i = 0; i < keys.length; i++) {
                felCtx.set(keys[i], 0f);
            }
            e = FEL_ENGINE.compile(expression, felCtx);
            DATA_FEL.put(expression, e);
        }
        com.greenpineyu.fel.context.ArrayCtxImpl felCtx = new com.greenpineyu.fel.context.ArrayCtxImpl();
        for (int i = 0; i < keys.length; i++) {
            felCtx.set(keys[i], values[i]);
        }
        Object result = e.eval(felCtx);
        if (result instanceof Double) {
            return ((Double) result).floatValue();
        } else if (result instanceof Float) {
            return ((Float) result).floatValue();
        } else {
            return Float.parseFloat(result.toString());
        }
    }

    /**
     * 计算表达式的值
     * evalex
     */
    public static float evalex(String expression, String[] keys, float[] values) {
        com.udojava.evalex.Expression e = null;
        if (DATA_EVALEX.containsKey(expression)) {
            e = DATA_EVALEX.get(expression);
        } else {
            e = new com.udojava.evalex.Expression(expression);
            DATA_EVALEX.put(expression, e);
        }
        for (int i = 0; i < keys.length; i++) {
            e.and(keys[i], new BigDecimal(values[i]));
        }
        try {
            return e.eval().floatValue();
        } catch (Exception ex) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ExceptionUtils.getRootCause(ex));
        }
    }


    /**
     * 计算表达式的值
     * exp4j
     */
    public static float exp4j(String expression, String[] keys, float[] values) {
        net.objecthunter.exp4j.Expression e = null;
        if (DATA_EXP4J.containsKey(expression)) {
            e = DATA_EXP4J.get(expression);
        } else {
            e = new net.objecthunter.exp4j.ExpressionBuilder(expression)
                    .operator(exp4j_operator_gt)
                    .operator(exp4j_operator_lt)
                    .operator(exp4j_operator_eq)
                    .operator(exp4j_operator_min)
                    .operator(exp4j_operator_max)
                    .variables(keys)
                    .build();
            DATA_EXP4J.put(expression, e);
        }

        for (int i = 0; i < keys.length; i++) {
            e.setVariable(keys[i], values[i]);
        }
        try {
            return (float) e.evaluate();
        } catch (Exception ex) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ExceptionUtils.getRootCause(ex));
        }
    }

    // 大于
    private static net.objecthunter.exp4j.operator.Operator exp4j_operator_gt = new net.objecthunter.exp4j.operator.Operator(">", 2, true, net.objecthunter.exp4j.operator.Operator.PRECEDENCE_ADDITION - 1) {
        @Override
        public double apply(double ... values) {
            if (values[0] > values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    };

    // 小于
    private static net.objecthunter.exp4j.operator.Operator exp4j_operator_lt = new net.objecthunter.exp4j.operator.Operator("<", 2, true, net.objecthunter.exp4j.operator.Operator.PRECEDENCE_ADDITION - 1) {
        @Override
        public double apply(double ... values) {
            if (values[0] < values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    };

    // 等于
    private static net.objecthunter.exp4j.operator.Operator exp4j_operator_eq = new net.objecthunter.exp4j.operator.Operator("=", 2, true, net.objecthunter.exp4j.operator.Operator.PRECEDENCE_ADDITION - 1) {
        @Override
        public double apply(double ... values) {
            if (values[0] == values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    };

    // 最小值
    private static net.objecthunter.exp4j.operator.Operator exp4j_operator_min = new net.objecthunter.exp4j.operator.Operator("<:", 2, true, net.objecthunter.exp4j.operator.Operator.PRECEDENCE_POWER + 1) {
        @Override
        public double apply(double ... values) {
            return Math.min(values[0], values[1]);
        }
    };

    // 最大值
    private static net.objecthunter.exp4j.operator.Operator exp4j_operator_max = new net.objecthunter.exp4j.operator.Operator(">:", 2, true, net.objecthunter.exp4j.operator.Operator.PRECEDENCE_POWER + 1) {
        @Override
        public double apply(double ... values) {
            return Math.max(values[0], values[1]);
        }
    };

}
