
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud.api.collections;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.Timer;
import org.apache.solr.cloud.OverseerTaskProcessor;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.util.stats.MetricUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverseerStatusCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;

  public OverseerStatusCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    ZkStateReader zkStateReader = ocmh.zkStateReader;
    String leaderNode = OverseerTaskProcessor.getLeaderNode(zkStateReader.getZkClient());
    results.add("leader", leaderNode);
    Stat stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/queue",null, stat, true);
    results.add("overseer_queue_size", stat.getNumChildren());
    stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/queue-work",null, stat, true);
    results.add("overseer_work_queue_size", stat.getNumChildren());
    stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/collection-queue-work",null, stat, true);
    results.add("overseer_collection_queue_size", stat.getNumChildren());

    // Overseer reported stats below are tracked in the Overseer cluster state updater when it performs certain operations.
    // Sharing the ocmh.stats variable between the cluster state updater and the Collection API (this command) is by the way
    // about the only thing that ties the cluster state updater to the collection api message handler and that takes
    // advantage of the fact that both run on the same node (the Overseer node). (recently added PerReplicaStates also
    // take advantage of this through method Overseer.submit()).
    // When distributed updates are enabled, cluster state updates are not done by the Overseer (it doesn't even see them)
    // and therefore can't report them. The corresponding data in OVERSEERSTATUS (all data built below) is no longer returned.
    // This means top level keys "overseer_operations", "collection_operations", "overseer_queue", "overseer_internal_queue"
    // and "collection_queue" are either empty or do not contain all expected information when cluster state updates are distributed.

    @SuppressWarnings({"rawtypes"})
    NamedList overseerStats = new NamedList();
    @SuppressWarnings({"rawtypes"})
    NamedList collectionStats = new NamedList();
    @SuppressWarnings({"rawtypes"})
    NamedList stateUpdateQueueStats = new NamedList();
    @SuppressWarnings({"rawtypes"})
    NamedList workQueueStats = new NamedList();
    @SuppressWarnings({"rawtypes"})
    NamedList collectionQueueStats = new NamedList();
    Stats stats = ocmh.stats;
    for (Map.Entry<String, Stats.Stat> entry : stats.getStats().entrySet()) {
      String key = entry.getKey();
      NamedList<Object> lst = new SimpleOrderedMap<>();
      if (key.startsWith("collection_"))  {
        collectionStats.add(key.substring(11), lst);
        int successes = stats.getSuccessCount(entry.getKey());
        int errors = stats.getErrorCount(entry.getKey());
        lst.add("requests", successes);
        lst.add("errors", errors);
        List<Stats.FailedOp> failureDetails = stats.getFailureDetails(key);
        if (failureDetails != null) {
          List<SimpleOrderedMap<Object>> failures = new ArrayList<>();
          for (Stats.FailedOp failedOp : failureDetails) {
            SimpleOrderedMap<Object> fail = new SimpleOrderedMap<>();
            fail.add("request", failedOp.req.getProperties());
            fail.add("response", failedOp.resp.getResponse());
            failures.add(fail);
          }
          lst.add("recent_failures", failures);
        }
      } else if (key.startsWith("/overseer/queue_"))  {
        stateUpdateQueueStats.add(key.substring(16), lst);
      } else if (key.startsWith("/overseer/queue-work_"))  {
        workQueueStats.add(key.substring(21), lst);
      } else if (key.startsWith("/overseer/collection-queue-work_"))  {
        collectionQueueStats.add(key.substring(32), lst);
      } else  {
        // overseer stats
        overseerStats.add(key, lst);
        int successes = stats.getSuccessCount(entry.getKey());
        int errors = stats.getErrorCount(entry.getKey());
        lst.add("requests", successes);
        lst.add("errors", errors);
      }
      Timer timer = entry.getValue().requestTime;
      MetricUtils.addMetrics(lst, timer);
    }
    results.add("overseer_operations", overseerStats);
    results.add("collection_operations", collectionStats);
    results.add("overseer_queue", stateUpdateQueueStats);
    results.add("overseer_internal_queue", workQueueStats);
    results.add("collection_queue", collectionQueueStats);

  }
}
