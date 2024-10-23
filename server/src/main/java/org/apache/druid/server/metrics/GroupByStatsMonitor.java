/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.metrics;

import com.google.inject.Inject;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.groupby.GroupByStatsProvider;

import java.nio.ByteBuffer;

public class GroupByStatsMonitor extends AbstractMonitor
{
  private final GroupByStatsProvider groupByStatsProvider;
  private final BlockingPool<ByteBuffer> mergeBufferPool;

  @Inject
  public GroupByStatsMonitor(
      GroupByStatsProvider groupByStatsProvider,
      @Merging BlockingPool<ByteBuffer> mergeBufferPool
  )
  {
    this.groupByStatsProvider = groupByStatsProvider;
    this.mergeBufferPool = mergeBufferPool;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();

    emitter.emit(builder.setMetric("mergeBuffer/pendingRequests", mergeBufferPool.getPendingRequests()));

    emitter.emit(builder.setMetric("mergeBuffer/usedCount", mergeBufferPool.getUsedResourcesCount()));

    Pair<Long, Long> groupByResourceAcquisitionStats =
        groupByStatsProvider.getAndResetMergeBufferAcquisitionStats();

    emitter.emit(builder.setMetric("mergeBuffer/acquisitionCount", groupByResourceAcquisitionStats.lhs));
    emitter.emit(builder.setMetric("mergeBuffer/acquisitionTimeNs", groupByResourceAcquisitionStats.rhs));

    Pair<Long, Long> spilledBytes = groupByStatsProvider.getAndResetSpilledBytes();
    emitter.emit(builder.setMetric("groupBy/spilledQueries", spilledBytes.lhs));
    emitter.emit(builder.setMetric("groupBy/spilledBytes", spilledBytes.rhs));

    return true;
  }
}