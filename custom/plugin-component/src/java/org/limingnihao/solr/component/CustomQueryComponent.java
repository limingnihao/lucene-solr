package org.limingnihao.solr.component;

import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

public class CustomQueryComponent extends QueryComponent {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        super.prepare(rb);
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
        super.process(rb);
    }


}
