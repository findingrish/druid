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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.timeline.SegmentId;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.TransactionCallback;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Handles segment schema persistence and cleanup.
 */
@LazySingleton
public class SegmentSchemaManager
{
  private static final EmittingLogger log = new EmittingLogger(SegmentSchemaManager.class);
  private static final int DB_ACTION_PARTITION_SIZE = 100;
  private final MetadataStorageTablesConfig dbTables;
  private final ObjectMapper jsonMapper;
  private final SQLMetadataConnector connector;

  @Inject
  public SegmentSchemaManager(
      MetadataStorageTablesConfig dbTables,
      ObjectMapper jsonMapper,
      SQLMetadataConnector connector
  )
  {
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
    this.connector = connector;
  }

  /**
   * Return a list of schema fingerprints
   */
  public List<String> findReferencedSchemaMarkedAsUnused()
  {
    return connector.retryWithHandle(
        handle ->
            handle.createQuery(
                      StringUtils.format(
                          "SELECT DISTINCT(schema_fingerprint) FROM %s WHERE used = true AND schema_fingerprint IN (SELECT fingerprint FROM %s WHERE used = false)",
                          dbTables.getSegmentsTable(),
                          dbTables.getSegmentSchemasTable()
                      ))
                  .mapTo(String.class)
                  .list()
    );
  }

  public int markSchemaAsUsed(List<String> schemaFingerprints)
  {
    if (schemaFingerprints.isEmpty()) {
      return 0;
    }
    String inClause = getInClause(schemaFingerprints.stream());

    return connector.retryWithHandle(
        handle ->
            handle.createStatement(
                      StringUtils.format(
                          "UPDATE %s SET used = true, used_status_last_updated = :now"
                          + " WHERE fingerprint IN (%s)",
                          dbTables.getSegmentSchemasTable(), inClause
                      )
                  )
                  .bind("now", DateTimes.nowUtc().toString())
                  .execute()
    );
  }

  public int deleteSchemasOlderThan(long timestamp)
  {
    return connector.retryWithHandle(
        handle -> handle.createStatement(
                            StringUtils.format(
                                "DELETE FROM %s WHERE used = false AND used_status_last_updated < :now",
                                dbTables.getSegmentSchemasTable()
                            ))
                        .bind("now", DateTimes.utc(timestamp).toString())
                        .execute());
  }

  public int markUnreferencedSchemasAsUnused()
  {
    return connector.retryWithHandle(
        handle ->
            handle.createStatement(
                      StringUtils.format(
                          "UPDATE %s SET used = false, used_status_last_updated = :now  WHERE used != false "
                          + "AND fingerprint NOT IN (SELECT DISTINCT(schema_fingerprint) FROM %s WHERE used=true AND schema_fingerprint IS NOT NULL)",
                          dbTables.getSegmentSchemasTable(),
                          dbTables.getSegmentsTable()
                      )
                  )
                  .bind("now", DateTimes.nowUtc().toString())
                  .execute());
  }

  /**
   * Persist segment schema and update segments in a transaction.
   */
  public void persistSchemaAndUpdateSegmentsTable(String dataSource, List<SegmentSchemaMetadataPlus> segmentSchemas, int schemaVersion)
  {
    connector.retryTransaction((TransactionCallback<Void>) (handle, status) -> {
      Map<String, SchemaPayload> schemaPayloadMap = new HashMap<>();

      for (SegmentSchemaMetadataPlus segmentSchema : segmentSchemas) {
        schemaPayloadMap.put(
            segmentSchema.getFingerprint(),
            segmentSchema.getSegmentSchemaMetadata().getSchemaPayload()
        );
      }
      persistSegmentSchema(handle, dataSource, schemaVersion, schemaPayloadMap);
      updateSegmentWithSchemaInformation(handle, segmentSchemas);

      return null;
    }, 1, 3);
  }

  /**
   * Persist unique segment schema in the DB.
   */
  public void persistSegmentSchema(
      Handle handle,
      String dataSource,
      int schemaVersion,
      Map<String, SchemaPayload> fingerprintSchemaPayloadMap
  ) throws JsonProcessingException
  {
    try {
      // Filter already existing schema
      Map<Boolean, Set<String>> existingFingerprintsAndUsedStatus = fingerprintExistBatch(handle, fingerprintSchemaPayloadMap.keySet());

      // Used schema can also be marked as unused by the schema cleanup duty in parallel.
      // Refer to the javadocs in org.apache.druid.server.coordinator.duty.KillUnreferencedSegmentSchemaDuty for more details.
      Set<String> usedExistingFingerprints = existingFingerprintsAndUsedStatus.containsKey(true) ? existingFingerprintsAndUsedStatus.get(true) : new HashSet<>();
      Set<String> unusedExistingFingerprints = existingFingerprintsAndUsedStatus.containsKey(false) ? existingFingerprintsAndUsedStatus.get(false) : new HashSet<>();
      Set<String> existingFingerprints = Sets.union(usedExistingFingerprints, unusedExistingFingerprints);
      if (existingFingerprints.size() > 0) {
        log.info(
            "Found already existing schema in the DB for dataSource [%1$s]. Used fingeprints: [%2$s], Unused fingerprints: [%3$s]",
            dataSource,
            usedExistingFingerprints,
            unusedExistingFingerprints
        );
      }

      // Unused schema can be deleted by the schema cleanup duty in parallel.
      // Refer to the javadocs in org.apache.druid.server.coordinator.duty.KillUnreferencedSegmentSchemaDuty for more details.
      if (unusedExistingFingerprints.size() > 0) {
        // make the unused schema as used to prevent deletion
        String inClause = getInClause(unusedExistingFingerprints.stream());

        handle.createStatement(
                  StringUtils.format(
                      "UPDATE %s SET used = true, used_status_last_updated = :now"
                      + " WHERE fingerprint IN (%s)",
                      dbTables.getSegmentSchemasTable(), inClause)
              )
              .bind("now", DateTimes.nowUtc().toString())
              .execute();
      }

      Map<String, SchemaPayload> schemaPayloadToCreate = new HashMap<>();

      for (Map.Entry<String, SchemaPayload> entry : fingerprintSchemaPayloadMap.entrySet()) {
        if (!existingFingerprints.contains(entry.getKey())) {
          schemaPayloadToCreate.put(entry.getKey(), entry.getValue());
        }
      }

      if (schemaPayloadToCreate.isEmpty()) {
        log.info("No schema to persist for dataSource [%s].", dataSource);
        return;
      }

      final List<List<String>> partitionedFingerprints = Lists.partition(
          new ArrayList<>(schemaPayloadToCreate.keySet()),
          DB_ACTION_PARTITION_SIZE
      );

      String insertSql = StringUtils.format(
          "INSERT INTO %s (created_date, datasource, fingerprint, payload, used, used_status_last_updated, version) "
          + "VALUES (:created_date, :datasource, :fingerprint, :payload, :used, :used_status_last_updated, :version)",
          dbTables.getSegmentSchemasTable()
      );

      // insert schemas
      PreparedBatch schemaInsertBatch = handle.prepareBatch(insertSql);
      for (List<String> partition : partitionedFingerprints) {
        for (String fingerprint : partition) {
          final String now = DateTimes.nowUtc().toString();
          schemaInsertBatch.add()
                           .bind("created_date", now)
                           .bind("datasource", dataSource)
                           .bind("fingerprint", fingerprint)
                           .bind("payload", jsonMapper.writeValueAsBytes(fingerprintSchemaPayloadMap.get(fingerprint)))
                           .bind("used", true)
                           .bind("used_status_last_updated", now)
                           .bind("version", schemaVersion);
        }
        final int[] affectedRows = schemaInsertBatch.execute();
        final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
        if (succeeded) {
          log.info("Published schemas to DB: %s", partition);
        } else {
          final List<String> failedToPublish =
              IntStream.range(0, partition.size())
                       .filter(i -> affectedRows[i] != 1)
                       .mapToObj(partition::get)
                       .collect(Collectors.toList());
          throw new ISE("Failed to publish schemas to DB: %s", failedToPublish);
        }
      }
    }
    catch (Exception e) {
      log.error("Exception inserting schemas to DB: %s", fingerprintSchemaPayloadMap);
      throw e;
    }
  }

  /**
   * Update segment with schemaId and numRows information.
   */
  public void updateSegmentWithSchemaInformation(Handle handle, List<SegmentSchemaMetadataPlus> batch)
  {
    log.debug("Updating segment with schema and numRows information: [%s].", batch);

    // update schemaId and numRows in segments table
    String updateSql =
        StringUtils.format(
            "UPDATE %s SET schema_fingerprint = :schema_fingerprint, num_rows = :num_rows WHERE id = :id",
            dbTables.getSegmentsTable()
        );

    PreparedBatch segmentUpdateBatch = handle.prepareBatch(updateSql);

    List<List<SegmentSchemaMetadataPlus>> partitionedSegmentIds =
        Lists.partition(
            batch,
            DB_ACTION_PARTITION_SIZE
        );

    for (List<SegmentSchemaMetadataPlus> partition : partitionedSegmentIds) {
      for (SegmentSchemaMetadataPlus segmentSchema : partition) {
        String fingerprint = segmentSchema.getFingerprint();

        segmentUpdateBatch.add()
                          .bind("id", segmentSchema.getSegmentId().toString())
                          .bind("schema_fingerprint", fingerprint)
                          .bind("num_rows", segmentSchema.getSegmentSchemaMetadata().getNumRows());
      }

      final int[] affectedRows = segmentUpdateBatch.execute();
      final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);

      if (succeeded) {
        log.info("Updated segments with schemaId & numRows in the DB: %s", partition);
      } else {
        final List<String> failedToUpdate =
            IntStream.range(0, partition.size())
                     .filter(i -> affectedRows[i] != 1)
                     .mapToObj(partition::get)
                     .map(plus -> plus.getSegmentId().toString())
                     .collect(Collectors.toList());
        throw new ISE("Failed to update segments with schema information: %s", failedToUpdate);
      }
    }
  }

  /**
   * Query the metadata DB to filter the fingerprints that exists.
   * It returns separate set for used and unused fingerprints in a map.
   */
  private Map<Boolean, Set<String>> fingerprintExistBatch(Handle handle, Set<String> fingerprintsToInsert)
  {
    List<List<String>> partitionedFingerprints = Lists.partition(
        new ArrayList<>(fingerprintsToInsert),
        DB_ACTION_PARTITION_SIZE
    );

    Map<Boolean, Set<String>> existingFingerprints = new HashMap<>();
    for (List<String> fingerprintList : partitionedFingerprints) {
      String fingerprints = fingerprintList.stream()
                                           .map(fingerprint -> "'" + StringEscapeUtils.escapeSql(fingerprint) + "'")
                                           .collect(Collectors.joining(","));
      handle.createQuery(
                StringUtils.format(
                    "SELECT used, fingerprint FROM %s WHERE fingerprint IN (%s)",
                    dbTables.getSegmentSchemasTable(), fingerprints
                )
            )
            .map((index, r, ctx) -> existingFingerprints.computeIfAbsent(
                r.getBoolean(1), value -> new HashSet<>()).add(r.getString(2)))
            .list();
    }
    return existingFingerprints;
  }

  private String getInClause(Stream<String> ids)
  {
    return ids
        .map(value -> "'" + StringEscapeUtils.escapeSql(value) + "'")
        .collect(Collectors.joining(","));
  }

  /**
   * Wrapper over {@link SchemaPayloadPlus} class to include segmentId and fingerprint information.
   */
  public static class SegmentSchemaMetadataPlus
  {
    private final SegmentId segmentId;
    private final String fingerprint;
    private final SchemaPayloadPlus schemaPayloadPlus;

    public SegmentSchemaMetadataPlus(
        SegmentId segmentId,
        String fingerprint,
        SchemaPayloadPlus schemaPayloadPlus
    )
    {
      this.segmentId = segmentId;
      this.schemaPayloadPlus = schemaPayloadPlus;
      this.fingerprint = fingerprint;
    }

    public SegmentId getSegmentId()
    {
      return segmentId;
    }

    public SchemaPayloadPlus getSegmentSchemaMetadata()
    {
      return schemaPayloadPlus;
    }

    public String getFingerprint()
    {
      return fingerprint;
    }

    @Override
    public String toString()
    {
      return "SegmentSchemaMetadataPlus{" +
             "segmentId='" + segmentId + '\'' +
             ", fingerprint='" + fingerprint + '\'' +
             ", segmentSchemaMetadata=" + schemaPayloadPlus +
             '}';
    }
  }
}
