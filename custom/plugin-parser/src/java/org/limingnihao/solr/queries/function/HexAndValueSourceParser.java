package org.limingnihao.solr.queries.function;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DualFloatFunction;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;


/**
 * 16进制计算函数
 * 将索引的值与入参的16进制做与运算，返回二进制序列
 * 两个入参
 */
public class HexAndValueSourceParser extends ValueSourceParser {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String name = "hexand";

    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new HexAndFunction(a, b);
    }

    public class HexAndFunction extends DualFloatFunction {
        /**
         * @param a the numerator.
         * @param b the denominator.
         */
        public HexAndFunction(ValueSource a, ValueSource b) {
            super(a, b);
        }

        @Override
        protected String name() {
            return name;
        }

        @Override
        protected float func(int doc, FunctionValues aVals, FunctionValues bVals) throws IOException {
            int a = aVals.intVal(doc);
            int b = bVals.intVal(doc);
            log.info("doc: {}, a: {}, b: {}", doc, a, b);
            return a | b;
        }
    }

}
