package org.limingnihao.solr.table.util;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import net.objecthunter.exp4j.operator.Operator;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

/**
 * 表达式计算
 * exp4j 开源项目: https://github.com/fasseg/exp4j
 */
public class EvalExp4jUtil {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final static HashMap<String, Expression> DATA_EXP4J = new HashMap<>(100);

    /**
     * 计算表达式的值
     * exp4j
     */
    public static float eval(String expression, Map<String, Float> params) {
        Expression e = null;
        if (DATA_EXP4J.containsKey(expression)) {
            e = DATA_EXP4J.get(expression);
        } else {
            e = new ExpressionBuilder(expression)
                    .operator(exp4j_operator_div)
                    .operator(exp4j_operator_gt)
                    .operator(exp4j_operator_lt)
                    .operator(exp4j_operator_eq)
                    .operator(exp4j_operator_min)
                    .operator(exp4j_operator_max)
                    .variables(params.keySet())
                    .build();
            DATA_EXP4J.put(expression, e);
        }
        for (Map.Entry<String, Float> v : params.entrySet()) {
            e.setVariable(v.getKey(), v.getValue());
        }
        try {
            return (float) e.evaluate();
        } catch (Exception ex) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ExceptionUtils.getMessage(ex));
        }
    }

    /**
     * 计算表达式的值
     * exp4j
     */
    public static float eval(String expression, String[] keys, float[] values) {
        Expression e = null;
        if (DATA_EXP4J.containsKey(expression)) {
            e = DATA_EXP4J.get(expression);
        } else {
            e = new ExpressionBuilder(expression)
                    .operator(exp4j_operator_div)
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
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ExceptionUtils.getMessage(ex));
        }
    }

    // 除法，带默认值
    private static Operator exp4j_operator_div = new Operator("//", 2, true, Operator.PRECEDENCE_ADDITION - 1) {
        @Override
        public double apply(double... values) {
            if (values[0] == 0 || values[1] == 0) {
                return 0d;
            } else {
                return values[0] / values[1];
            }
        }
    };

    // 大于
    private static Operator exp4j_operator_gt = new Operator(">", 2, true, Operator.PRECEDENCE_ADDITION - 1) {
        @Override
        public double apply(double... values) {
            if (values[0] > values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    };

    // 小于
    private static Operator exp4j_operator_lt = new Operator("<", 2, true, Operator.PRECEDENCE_ADDITION - 1) {
        @Override
        public double apply(double... values) {
            if (values[0] < values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    };

    // 等于
    private static Operator exp4j_operator_eq = new Operator("=", 2, true, Operator.PRECEDENCE_ADDITION - 1) {
        @Override
        public double apply(double... values) {
            if (values[0] == values[1]) {
                return 1d;
            } else {
                return 0d;
            }
        }
    };

    // 最小值
    private static Operator exp4j_operator_min = new Operator("<:", 2, true, Operator.PRECEDENCE_POWER + 1) {
        @Override
        public double apply(double... values) {
            return Math.min(values[0], values[1]);
        }
    };

    // 最大值
    private static Operator exp4j_operator_max = new Operator(">:", 2, true, Operator.PRECEDENCE_POWER + 1) {
        @Override
        public double apply(double... values) {
            return Math.max(values[0], values[1]);
        }
    };

}
