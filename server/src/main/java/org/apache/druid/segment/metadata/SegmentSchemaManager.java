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
import com.google.inject.Inject;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SegmentSchemaMetadata;
import org.apache.druid.timeline.SegmentId;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.Update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
   * Remove segment schema which are no longer referenced by any segment.
   */
  public int cleanUpUnreferencedSchema()
  {
    return connector.retryTransaction(
        (handle, transactionStatus) -> {
          Update deleteStatement =
              handle.createStatement(
                  StringUtils.format(
                      "DELETE FROM %1$s WHERE id NOT IN (SELECT distinct(schema_id) FROM %2$s where schema_id is not null)",
                      dbTables.getSegmentSchemaTable(),
                      dbTables.getSegmentsTable()
                  ));
          return deleteStatement.execute();
        }, 1, 3
    );
  }

  /**
   * Persist segment schema and update segments in a transaction.
   */
  public void persistSchemaAndUpdateSegmentsTable(List<SegmentSchemaMetadataPlus> segmentSchemas)
  {
    connector.retryTransaction((TransactionCallback<Void>) (handle, status) -> {
      Map<String, SchemaPayload> schemaPayloadMap = new HashMap<>();

      for (SegmentSchemaMetadataPlus segmentSchema : segmentSchemas) {
        schemaPayloadMap.put(
            segmentSchema.getFingerprint(),
            segmentSchema.getSegmentSchemaMetadata().getSchemaPayload()
        );
      }
      persistSegmentSchema(handle, schemaPayloadMap);
      updateSegmentWithSchemaInformation(handle, segmentSchemas);

      return null;
    }, 1, 3);
  }

  /**
   * Persist unique segment schema in the DB.
   */
  public void persistSegmentSchema(Handle handle, Map<String, SchemaPayload> fingerprintSchemaPayloadMap)
      throws JsonProcessingException
  {
    try {
      // Filter already existing schema
      Set<String> existingFingerprint = fingerprintExistBatch(handle, fingerprintSchemaPayloadMap.keySet());
      log.info("Found already existing schema in the DB: %s", existingFingerprint);

      Map<String, SchemaPayload> filteredSchemaPayloadMap = new HashMap<>();

      for (Map.Entry<String, SchemaPayload> entry : fingerprintSchemaPayloadMap.entrySet()) {
        if (!existingFingerprint.contains(entry.getKey())) {
          filteredSchemaPayloadMap.put(entry.getKey(), entry.getValue());
        }
      }

      if (filteredSchemaPayloadMap.isEmpty()) {
        log.info("No schema to persist.");
        return;
      }

      final List<List<String>> partitionedFingerprints = Lists.partition(
          new ArrayList<>(filteredSchemaPayloadMap.keySet()),
          DB_ACTION_PARTITION_SIZE
      );

      String insertSql = StringUtils.format(
          "INSERT INTO %1$s (fingerprint, created_date, payload) "
          + "VALUES (:fingerprint, :created_date, :payload)",
          dbTables.getSegmentSchemaTable()
      );

      // insert schemas
      PreparedBatch schemaInsertBatch = handle.prepareBatch(insertSql);
      for (List<String> partition : partitionedFingerprints) {
        for (String fingerprint : partition) {
          final String now = DateTimes.nowUtc().toString();
          schemaInsertBatch.add()
                           .bind("created_date", now)
                           .bind("fingerprint", fingerprint)
                           .bind("payload", jsonMapper.writeValueAsBytes(fingerprintSchemaPayloadMap.get(fingerprint)));
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

    // segments which are already updated with the schema information.
    Set<String> updatedSegments =
        segmentUpdatedBatch(handle, batch.stream().map(plus -> plus.getSegmentId().toString()).collect(Collectors.toSet()));

    log.debug("Already updated segments: [%s].", updatedSegments);

    List<SegmentSchemaMetadataPlus> segmentsToUpdate =
        batch.stream()
             .filter(id -> !updatedSegments.contains(id.getSegmentId().toString()))
             .collect(Collectors.toList());

    // fetch schemaId
    Map<String, Long> fingerprintSchemaIdMap =
        schemaIdFetchBatch(
            handle,
            segmentsToUpdate
                .stream()
                .map(SegmentSchemaMetadataPlus::getFingerprint)
                .collect(Collectors.toSet())
        );

    log.debug("FingerprintSchemaIdMap: [%s].", fingerprintSchemaIdMap);

    // update schemaId and numRows in segments table
    String updateSql =
        StringUtils.format(
            "UPDATE %s SET schema_id = :schema_id, num_rows = :num_rows where id = :id",
            dbTables.getSegmentsTable()
        );

    PreparedBatch segmentUpdateBatch = handle.prepareBatch(updateSql);

    List<List<SegmentSchemaMetadataPlus>> partitionedSegmentIds =
        Lists.partition(
            segmentsToUpdate,
            DB_ACTION_PARTITION_SIZE
        );

    for (List<SegmentSchemaMetadataPlus> partition : partitionedSegmentIds) {
      for (SegmentSchemaMetadataPlus segmentSchema : segmentsToUpdate) {
        String fingerprint = segmentSchema.getFingerprint();
        if (!fingerprintSchemaIdMap.containsKey(fingerprint)) {
          log.error("Fingerprint [%s] for segmentId [%s] is not associated with any schemaId.", fingerprint, segmentSchema.getSegmentId());
          continue;
        }

        segmentUpdateBatch.add()
                          .bind("id", segmentSchema.getSegmentId().toString())
                          .bind("schema_id", fingerprintSchemaIdMap.get(fingerprint))
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

  private Set<String> fingerprintExistBatch(Handle handle, Set<String> fingerprintsToInsert)
  {
    List<List<String>> partitionedFingerprints = Lists.partition(
        new ArrayList<>(fingerprintsToInsert),
        DB_ACTION_PARTITION_SIZE
    );

    Set<String> existingSchemas = new HashSet<>();
    for (List<String> fingerprintList : partitionedFingerprints) {
      String fingerprints = fingerprintList.stream()
                 .map(fingerprint -> "'" + StringEscapeUtils.escapeSql(fingerprint) + "'")
                 .collect(Collectors.joining(","));
      List<String> existIds =
          handle.createQuery(
                    StringUtils.format(
                        "SELECT fingerprint FROM %s WHERE fingerprint in (%s)",
                        dbTables.getSegmentSchemaTable(), fingerprints
                    )
                )
                .mapTo(String.class)
                .list();
      existingSchemas.addAll(existIds);
    }
    return existingSchemas;
  }

  public Map<String, Long> schemaIdFetchBatch(Handle handle, Set<String> fingerprintsToQuery)
  {
    List<List<String>> partitionedFingerprints = Lists.partition(
        new ArrayList<>(fingerprintsToQuery),
        DB_ACTION_PARTITION_SIZE
    );

    Map<String, Long> fingerprintIdMap = new HashMap<>();
    for (List<String> fingerprintList : partitionedFingerprints) {
      String fingerprints = fingerprintList.stream()
                                           .map(fingerprint -> "'" + StringEscapeUtils.escapeSql(fingerprint) + "'")
                                           .collect(Collectors.joining(","));
      Map<String, Long> partitionFingerprintIdMap =
          handle.createQuery(
                    StringUtils.format(
                        "SELECT fingerprint, id FROM %s WHERE fingerprint in (%s)",
                        dbTables.getSegmentSchemaTable(), fingerprints
                    )
                )
                .map((index, r, ctx) -> Pair.of(r.getString("fingerprint"), r.getLong("id")))
                .fold(
                    new HashMap<>(), (accumulator, rs, control, ctx) -> {
                      accumulator.put(rs.lhs, rs.rhs);
                      return accumulator;
                    }
                );
      fingerprintIdMap.putAll(partitionFingerprintIdMap);
    }
    return fingerprintIdMap;
  }

  private Set<String> segmentUpdatedBatch(Handle handle, Set<String> segmentIds)
  {
    List<List<String>> partitionedSegmentIds = Lists.partition(
        new ArrayList<>(segmentIds),
        DB_ACTION_PARTITION_SIZE
    );

    Set<String> updated = new HashSet<>();
    for (List<String> partition : partitionedSegmentIds) {
      String ids = partition.stream()
                            .map(id -> "'" + StringEscapeUtils.escapeSql(id) + "'")
                            .collect(Collectors.joining(","));
      List<String> updatedSegmentIds =
          handle.createQuery(
                    StringUtils.format(
                        "SELECT id from %s where id in (%s) and schema_id IS NOT NULL and num_rows IS NOT NULL",
                        dbTables.getSegmentsTable(),
                        ids
                    ))
                .mapTo(String.class)
                .list();

      updated.addAll(updatedSegmentIds);
    }
    return updated;
  }

  /**
   * Wrapper over {@link SegmentSchemaMetadata} class to include segmentId and fingerprint information.
   */
  public static class SegmentSchemaMetadataPlus
  {
    private final SegmentId segmentId;
    private final String fingerprint;
    private final SegmentSchemaMetadata segmentSchemaMetadata;

    public SegmentSchemaMetadataPlus(
        SegmentId segmentId,
        String fingerprint,
        SegmentSchemaMetadata segmentSchemaMetadata
    )
    {
      this.segmentId = segmentId;
      this.segmentSchemaMetadata = segmentSchemaMetadata;
      this.fingerprint = fingerprint;
    }

    public SegmentId getSegmentId()
    {
      return segmentId;
    }

    public SegmentSchemaMetadata getSegmentSchemaMetadata()
    {
      return segmentSchemaMetadata;
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
             ", segmentSchemaMetadata=" + segmentSchemaMetadata +
             '}';
    }
  }
}
