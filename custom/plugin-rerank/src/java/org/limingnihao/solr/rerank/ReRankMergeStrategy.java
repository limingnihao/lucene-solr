package org.limingnihao.solr.rerank;

import org.apache.lucene.search.SortField;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.*;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

/**
 * rerank  merge
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class ReRankMergeStrategy implements MergeStrategy {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void merge(ResponseBuilder rb, ShardRequest sreq) {
        merge_id(rb, sreq);
    }

    @Override
    public boolean mergesIds() {
        return true;
    }

    @Override
    public boolean handlesMergeFields() {
        return false;
    }

    @Override
    public void handleMergeFields(ResponseBuilder rb, SolrIndexSearcher searcher) throws IOException {
    }

    @Override
    public int getCost() {
        return 0;
    }


    public void merge_id(ResponseBuilder rb, ShardRequest sreq) {
        SortSpec ss = rb.getSortSpec();

        SortField[] sortFields  = new SortField[]{SortField.FIELD_SCORE};

        IndexSchema schema = rb.req.getSchema();
        SchemaField uniqueKeyField = schema.getUniqueKeyField();


        // id to shard mapping, to eliminate any accidental dups
        HashMap<Object, String> uniqueDoc = new HashMap<>();

        // Merge the docs via a priority queue so we don't have to sort *all* of the
        // documents... we only need to order the top (rows+start)
        final ShardFieldSortedHitQueue queue = new ShardFieldSortedHitQueue(sortFields, ss.getOffset() + ss.getCount(), rb.req.getSearcher());

        NamedList<Object> shardInfo = null;
        if (rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
            shardInfo = new SimpleOrderedMap<>();
            rb.rsp.getValues().add(ShardParams.SHARDS_INFO, shardInfo);
        }

        long numFound = 0;
        Float maxScore = null;
        boolean thereArePartialResults = false;
        Boolean segmentTerminatedEarly = null;
        for (ShardResponse srsp : sreq.responses) {
            SolrDocumentList docs = null;
            NamedList<?> responseHeader = null;

            if (shardInfo != null) {
                SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();

                if (srsp.getException() != null) {
                    Throwable t = srsp.getException();
                    if (t instanceof SolrServerException) {
                        t = ((SolrServerException) t).getCause();
                    }
                    nl.add("error", t.toString());
                    StringWriter trace = new StringWriter();
                    t.printStackTrace(new PrintWriter(trace));
                    nl.add("trace", trace.toString());
                    if (srsp.getShardAddress() != null) {
                        nl.add("shardAddress", srsp.getShardAddress());
                    }
                } else {
                    responseHeader = (NamedList<?>) srsp.getSolrResponse().getResponse().get("responseHeader");
                    final Object rhste = responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
                    if (rhste != null) {
                        nl.add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, rhste);
                    }
                    docs = (SolrDocumentList) srsp.getSolrResponse().getResponse().get("response");
                    nl.add("numFound", docs.getNumFound());
                    nl.add("numTotal", docs.size());
                    nl.add("maxScore", docs.getMaxScore());
                    nl.add("shardAddress", srsp.getShardAddress());
                }
                if (srsp.getSolrResponse() != null) {
                    nl.add("time", srsp.getSolrResponse().getElapsedTime());
                }

                shardInfo.add(srsp.getShard(), nl);
            }
            // now that we've added the shard info, let's only proceed if we have no error.
            if (srsp.getException() != null) {
                thereArePartialResults = true;
                continue;
            }

            if (docs == null) { // could have been initialized in the shards info block above
                docs = (SolrDocumentList) srsp.getSolrResponse().getResponse().get("response");
            }

            if (responseHeader == null) { // could have been initialized in the shards info block above
                responseHeader = (NamedList<?>) srsp.getSolrResponse().getResponse().get("responseHeader");
            }

            final boolean thisResponseIsPartial;
            thisResponseIsPartial = Boolean.TRUE.equals(responseHeader.getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
            thereArePartialResults |= thisResponseIsPartial;

            if (!Boolean.TRUE.equals(segmentTerminatedEarly)) {
                final Object ste = responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
                if (Boolean.TRUE.equals(ste)) {
                    segmentTerminatedEarly = Boolean.TRUE;
                } else if (Boolean.FALSE.equals(ste)) {
                    segmentTerminatedEarly = Boolean.FALSE;
                }
            }

            // calculate global maxScore and numDocsFound
            if (docs.getMaxScore() != null) {
                maxScore = maxScore == null ? docs.getMaxScore() : Math.max(maxScore, docs.getMaxScore());
            }
            numFound += docs.getNumFound();

            // go through every doc in this response, construct a ShardDoc, and
            // put it in the priority queue so it can be ordered.
            for (int i = 0; i < docs.size(); i++) {
                SolrDocument doc = docs.get(i);
                Object id = doc.getFieldValue(uniqueKeyField.getName());

                String prevShard = uniqueDoc.put(id, srsp.getShard());
                if (prevShard != null) {
                    // duplicate detected
                    numFound--;

                    // For now, just always use the first encountered since we can't currently
                    // remove the previous one added to the priority queue.  If we switched
                    // to the Java5 PriorityQueue, this would be easier.
                    continue;
                    // make which duplicate is used deterministic based on shard
                    // if (prevShard.compareTo(srsp.shard) >= 0) {
                    //  TODO: remove previous from priority queue
                    //  continue;
                    // }
                }

                ShardDoc shardDoc = new ShardDoc();
                shardDoc.id = id;
                shardDoc.shard = srsp.getShard();
                shardDoc.orderInShard = i;
                Object scoreObj = doc.getFieldValue("score");
                if (scoreObj != null) {
                    if (scoreObj instanceof String) {
                        shardDoc.score = Float.parseFloat((String) scoreObj);
                    } else {
                        shardDoc.score = (Float) scoreObj;
                    }
                }
                queue.insertWithOverflow(shardDoc);
            } // end for-each-doc-in-response
        } // end for-each-response

        // The queue now has 0 -> queuesize docs, where queuesize <= start + rows
        // So we want to pop the last documents off the queue to get
        // the docs offset -> queuesize
        int resultSize = queue.size() - ss.getOffset();
        resultSize = Math.max(0, resultSize);  // there may not be any docs in range

        Map<Object, ShardDoc> resultIds = new HashMap<>();
        for (int i = resultSize - 1; i >= 0; i--) {
            ShardDoc shardDoc = queue.pop();
            shardDoc.positionInResponse = i;
            // Need the toString() for correlation with other lists that must
            // be strings (like keys in highlighting, explain, etc)
            resultIds.put(shardDoc.id.toString(), shardDoc);
        }

        // Add hits for distributed requests
        // https://issues.apache.org/jira/browse/SOLR-3518
        rb.rsp.addToLog("hits", numFound);

        SolrDocumentList responseDocs = new SolrDocumentList();
        if (maxScore != null) responseDocs.setMaxScore(maxScore);
        responseDocs.setNumFound(numFound);
        responseDocs.setStart(ss.getOffset());
        // size appropriately
        for (int i = 0; i < resultSize; i++) responseDocs.add(null);

        // save these results in a private area so we can access them
        // again when retrieving stored fields.
        // TODO: use ResponseBuilder (w/ comments) or the request context?
        rb.resultIds = resultIds;
        rb.setResponseDocs(responseDocs);

        if (thereArePartialResults) {
            rb.rsp.getResponseHeader().asShallowMap()
                    .put(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
        }
        if (segmentTerminatedEarly != null) {
            final Object existingSegmentTerminatedEarly = rb.rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
            if (existingSegmentTerminatedEarly == null) {
                rb.rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, segmentTerminatedEarly);
            } else if (!Boolean.TRUE.equals(existingSegmentTerminatedEarly) && Boolean.TRUE.equals(segmentTerminatedEarly)) {
                rb.rsp.getResponseHeader().remove(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
                rb.rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, segmentTerminatedEarly);
            }
        }
    }
}
