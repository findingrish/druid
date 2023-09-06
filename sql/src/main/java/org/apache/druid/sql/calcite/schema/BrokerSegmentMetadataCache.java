package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.metadata.DataSourceSchema;
import org.apache.druid.segment.metadata.SegmentMetadataCache;
import org.apache.druid.segment.metadata.SegmentMetadataCacheConfig;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.timeline.SegmentId;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
@ManageLifecycle
public class BrokerSegmentMetadataCache extends SegmentMetadataCache
{
  private static final EmittingLogger log = new EmittingLogger(BrokerSegmentMetadataCache.class);
  private final PhysicalDatasourceMetadataBuilder physicalDatasourceMetadataBuilder;

  private final ConcurrentMap<String, DatasourceTable.PhysicalDatasourceMetadata> tables = new ConcurrentHashMap<>();

  private final CoordinatorClient coordinatorClient;

  @Inject
  public BrokerSegmentMetadataCache(
      final QueryLifecycleFactory queryLifecycleFactory,
      final TimelineServerView serverView,
      final SegmentMetadataCacheConfig config,
      final Escalator escalator,
      final InternalQueryConfig internalQueryConfig,
      final ServiceEmitter emitter,
      final PhysicalDatasourceMetadataBuilder physicalDatasourceMetadataBuilder,
      final CoordinatorClient coordinatorClient
  )
  {
    super(
        queryLifecycleFactory,
        serverView,
        config,
        escalator,
        internalQueryConfig,
        emitter
    );
    this.physicalDatasourceMetadataBuilder = physicalDatasourceMetadataBuilder;
    this.coordinatorClient = coordinatorClient;
  }

  @Override
  public void refresh(final Set<SegmentId> segmentsToRefresh, final Set<String> dataSourcesToRebuild) throws IOException
  {
    Set<String> dataSourcesToQuery = new HashSet<>();

    segmentsToRefresh.forEach(segment -> dataSourcesToQuery.add(segment.getDataSource()));

    Map<String, DatasourceTable.PhysicalDatasourceMetadata> polledDataSourceSchema = new HashMap<>();

    // Fetch dataSource schema from the Coordinator
    try {
      FutureUtils.getUnchecked(coordinatorClient.fetchDataSourceSchema(dataSourcesToQuery), true)
                 .forEach(item -> polledDataSourceSchema.put(
                     item.getDatasource(),
                     physicalDatasourceMetadataBuilder.build(
                         item.getDatasource(),
                         item.getRowSignature()
                     )
                 ));
    } catch (Exception e) {
      log.error("Exception querying coordinator for schema");
    }

    log.info("Queried ds schema are [%s]", polledDataSourceSchema);

    tables.putAll(polledDataSourceSchema);

    // Remove segments of the dataSource from refresh list for which we received schema from the Coordinator.
    segmentsToRefresh.forEach(segment -> {
      if (polledDataSourceSchema.containsKey(segment.getDataSource())) {
        segmentsToRefresh.remove(segment);
      }
    });

    // Refresh the segments.
    final Set<SegmentId> refreshed = refreshSegments(segmentsToRefresh);

    synchronized (getLock()) {
      // Add missing segments back to the refresh list.
      getSegmentsNeedingRefresh().addAll(Sets.difference(segmentsToRefresh, refreshed));

      // Compute the list of dataSources to rebuild tables for.
      dataSourcesToRebuild.addAll(getDataSourcesNeedingRebuild());
      refreshed.forEach(segment -> dataSourcesToRebuild.add(segment.getDataSource()));

      // Remove those dataSource for which we received schema from the Coordinator.
      dataSourcesToRebuild.removeAll(polledDataSourceSchema.keySet());
      getDataSourcesNeedingRebuild().clear();
    }

    // Rebuild the dataSources.
    for (String dataSource : dataSourcesToRebuild) {
      rebuildDatasource(dataSource);
    }
  }

  @Override
  public void rebuildDatasource(String dataSource)
  {
    final DataSourceSchema druidTable = buildDruidTable(dataSource);
    if (druidTable == null) {
      log.info("dataSource [%s] no longer exists, all metadata removed.", dataSource);
      tables.remove(dataSource);
      return;
    }
    final DatasourceTable.PhysicalDatasourceMetadata physicalDatasourceMetadata =
        physicalDatasourceMetadataBuilder.build(dataSource, druidTable.getRowSignature());
    final DatasourceTable.PhysicalDatasourceMetadata oldTable = tables.put(dataSource, physicalDatasourceMetadata);
    if (oldTable == null || !oldTable.rowSignature().equals(physicalDatasourceMetadata.rowSignature())) {
      log.info("[%s] has new signature: %s.", dataSource, druidTable.getRowSignature());
    } else {
      log.info("[%s] signature is unchanged.", dataSource);
    }
  }

  @Override
  public Set<String> getDatasourceNames()
  {
    return tables.keySet();
  }

  @Override
  public void removeFromTable(String s)
  {
    tables.remove(s);
  }

  @Override
  public boolean tablesContains(String s)
  {
    return tables.containsKey(s);
  }

  public DatasourceTable.PhysicalDatasourceMetadata getPhysicalDatasourceMetadata(String name)
  {
    return tables.get(name);
  }
}
