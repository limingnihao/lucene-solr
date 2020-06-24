package org.limingnihao.solr.queries.function.test;

import org.apache.solr.search.*;
import org.apache.solr.search.join.*;
import org.apache.solr.search.mlt.MLTQParserPlugin;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestCalulate {

    @Test
    public void test() {
        String a = "1+2*4/5";
        String[] a1 = a.split("[\\+\\-\\*\\/]");
        String c = a.replaceAll("[\\+\\-\\*\\/]", "{}");

        System.out.println(Arrays.toString(a1));
        System.out.println(c);
    }


    @Test
    public void test_list() {
        HashMap<String, QParserPlugin> map = new LinkedHashMap<>(30, 1);
        map.put(LuceneQParserPlugin.NAME, new LuceneQParserPlugin());
        map.put(FunctionQParserPlugin.NAME, new FunctionQParserPlugin());
        map.put(PrefixQParserPlugin.NAME, new PrefixQParserPlugin());
        map.put(BoostQParserPlugin.NAME, new BoostQParserPlugin());
        map.put(DisMaxQParserPlugin.NAME, new DisMaxQParserPlugin());
        map.put(ExtendedDismaxQParserPlugin.NAME, new ExtendedDismaxQParserPlugin());
        map.put(FieldQParserPlugin.NAME, new FieldQParserPlugin());
        map.put(RawQParserPlugin.NAME, new RawQParserPlugin());
        map.put(TermQParserPlugin.NAME, new TermQParserPlugin());
        map.put(TermsQParserPlugin.NAME, new TermsQParserPlugin());
        map.put(NestedQParserPlugin.NAME, new NestedQParserPlugin());
        map.put(FunctionRangeQParserPlugin.NAME, new FunctionRangeQParserPlugin());
        map.put(SpatialFilterQParserPlugin.NAME, new SpatialFilterQParserPlugin());
        map.put(SpatialBoxQParserPlugin.NAME, new SpatialBoxQParserPlugin());
        map.put(JoinQParserPlugin.NAME, new JoinQParserPlugin());
        map.put(SurroundQParserPlugin.NAME, new SurroundQParserPlugin());
        map.put(SwitchQParserPlugin.NAME, new SwitchQParserPlugin());
        map.put(MaxScoreQParserPlugin.NAME, new MaxScoreQParserPlugin());
        map.put(BlockJoinParentQParserPlugin.NAME, new BlockJoinParentQParserPlugin());
        map.put(BlockJoinChildQParserPlugin.NAME, new BlockJoinChildQParserPlugin());
        map.put(FiltersQParserPlugin.NAME, new FiltersQParserPlugin());
        map.put(CollapsingQParserPlugin.NAME, new CollapsingQParserPlugin());
        map.put(SimpleQParserPlugin.NAME, new SimpleQParserPlugin());
        map.put(ComplexPhraseQParserPlugin.NAME, new ComplexPhraseQParserPlugin());
        map.put(ReRankQParserPlugin.NAME, new ReRankQParserPlugin());
        map.put(ExportQParserPlugin.NAME, new ExportQParserPlugin());
        map.put(MLTQParserPlugin.NAME, new MLTQParserPlugin());
        map.put(HashQParserPlugin.NAME, new HashQParserPlugin());
        map.put(GraphQParserPlugin.NAME, new GraphQParserPlugin());
        map.put(XmlQParserPlugin.NAME, new XmlQParserPlugin());
        map.put(GraphTermsQParserPlugin.NAME, new GraphTermsQParserPlugin());
        map.put(IGainTermsQParserPlugin.NAME, new IGainTermsQParserPlugin());
        map.put(TextLogisticRegressionQParserPlugin.NAME, new TextLogisticRegressionQParserPlugin());
        map.put(SignificantTermsQParserPlugin.NAME, new SignificantTermsQParserPlugin());
        map.put(PayloadScoreQParserPlugin.NAME, new PayloadScoreQParserPlugin());
        map.put(PayloadCheckQParserPlugin.NAME, new PayloadCheckQParserPlugin());
        map.put(BoolQParserPlugin.NAME, new BoolQParserPlugin());
        map.put(MinHashQParserPlugin.NAME, new MinHashQParserPlugin());
        map.put(XCJFQParserPlugin.NAME, new XCJFQParserPlugin());
        map.put(HashRangeQParserPlugin.NAME, new HashRangeQParserPlugin());

        for (Map.Entry<String, QParserPlugin> entry : map.entrySet()) {
            System.out.println(entry.getKey() + ", " + entry.getValue().getClass().getSimpleName());
        }
    }


}
