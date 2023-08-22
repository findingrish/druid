package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentSchema;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skife.jdbi.v2.PreparedBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestSegmentSchema
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  private TestDerbyConnector derbyConnector;

  private final DataSegment existingSegment1 = new DataSegment(
      "fooDataSource",
      Intervals.of("1994-01-01T00Z/1994-01-02T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(1, 1),
      9,
      100
  );

  private final DataSegment existingSegment2 = new DataSegment(
      "fooDataSource",
      Intervals.of("1994-01-02T00Z/1994-01-03T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(1, 1),
      9,
      100
  );

  @Before
  public void setUp()
  {
    derbyConnector = derbyConnectorRule.getConnector();
    mapper.registerSubtypes(LinearShardSpec.class, NumberedShardSpec.class, HashBasedNumberedShardSpec.class);
  }

  @Test
  public void test() {
    insertUsedSegmentAndSchema(Sets.newHashSet(existingSegment1, existingSegment2));
  }

  @Test
  public void test1() {
    List<String> l = Lists.newArrayList("schema_id_1", "schema_id_2");
//    String q = StringUtils.format(
//        "SELECT fingerprint FROM %s WHERE fingerprint in (%s)",
//        "DRUID_SEGMENTSCHEMA",
//                          l.stream().map(str -> String.format("'%s'", str)).collect(Collectors.joining(", "))
//    );
//    System.out.println(q);
//    derbyConnector.retryWithHandle(
//        handle -> {
//          List<String> z = handle
//              .createQuery(q)
//              .mapTo(String.class)
//              .list();
//          System.out.println(z);
//          return null;
//        }
//    );

    String query = StringUtils.format(
        "SELECT datasource, payload FROM %s WHERE fingerprint in (%s)",
        "DRUID_SEGMENTSCHEMA",
        l.stream().map(str -> String.format("'%s'", str)).collect(Collectors.joining(", "))
    );
    System.out.println(query);
    List<String> l1 = new ArrayList<>();
    derbyConnector
        .retryWithHandle(
            handle -> {
              handle.createQuery(query)
                    .setFetchSize(100)
                    .map(
                        (index, r, ctx) -> {
                          try {
                            System.out.println("atleast inside map");
                            String dataSource = r.getString("datasource");
                            System.out.println(dataSource);
                            SegmentSchema segmentSchema = mapper.readValue(
                                r.getBytes("payload"),
                                SegmentSchema.class
                            );
                            System.out.println(segmentSchema);
                          }
                          catch (IOException e) {
                            throw new RuntimeException(e);
                          }
                          return null;
                        }).list();
              return null;
  });
    Map<String, Set<SegmentSchema>> m = new HashMap<>();
    derbyConnector.retryWithHandle(
        handle -> {
        handle.createQuery(StringUtils.format(
            "SELECT datasource, payload FROM %s WHERE fingerprint in (%s)",
            "DRUID_SEGMENTSCHEMA",
            l.stream().map(str -> String.format("'%s'", str)).collect(Collectors.joining(", "))
        ))
        .setFetchSize(100)
        .map(
            (index, r, ctx) -> {
              try {
                SegmentSchema segmentSchema = mapper.readValue(
                    r.getBytes("payload"),
                    SegmentSchema.class
                );
                String dataSource = r.getString("datasource");
                m.computeIfAbsent(dataSource, set -> new HashSet<>()).add(segmentSchema);
              }
              catch (IOException e) {
               // log.makeAlert(e, "Failed to read segment schema from db.").emit();
              }
              return null;
            })
        .list();
        return null;
        });

    System.out.println("this is m new method" + m);
  }

  private Boolean insertUsedSegmentAndSchema(Set<DataSegment> dataSegments)
  {
    final String table = "DRUID_SEGMENTS";
    final String schemaTable = "DRUID_SEGMENTSCHEMA";
    return derbyConnector.retryWithHandle(
        handle -> {
          PreparedBatch preparedBatch = handle.prepareBatch(
              StringUtils.format(
                  "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, schema_id, payload) "
                  + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :schema_id, :payload)",
                  table,
                  derbyConnector.getQuoteString()
              )
          );
          int i = 1;
          for (DataSegment segment : dataSegments) {
            String schemaId = "schema_id_" + i;
            preparedBatch.add()
                         .bind("id", segment.getId().toString())
                         .bind("dataSource", segment.getDataSource())
                         .bind("created_date", DateTimes.nowUtc().toString())
                         .bind("start", segment.getInterval().getStart().toString())
                         .bind("end", segment.getInterval().getEnd().toString())
                         .bind("partitioned", !(segment.getShardSpec() instanceof NoneShardSpec))
                         .bind("version", segment.getVersion())
                         .bind("used", true)
                         .bind("schema_id", schemaId)
                         .bind("payload", mapper.writeValueAsBytes(segment));
            i++;
          }

          final int[] affectedRows = preparedBatch.execute();
          final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
          if (!succeeded) {
            throw new ISE("Failed to publish segments to DB");
          }

          /**
           StringUtils.format(
           "CREATE TABLE %1$s (\n"
           + "  fingerprint VARCHAR(255) NOT NULL,\n"
           + "  created_date VARCHAR(255) NOT NULL,\n"
           + "  datasource VARCHAR(255) %2$s NOT NULL,\n"
           + "  realtime BOOLEAN NOT NULL,\n"
           + "  payload %3$s NOT NULL,\n"
           + "  PRIMARY KEY (fingerprint)\n"
           + ")",
           */
          PreparedBatch preparedBatch1 = handle.prepareBatch(
              StringUtils.format(
                  "INSERT INTO %1$s (fingerprint, created_date, datasource, realtime, payload) "
                  + "VALUES (:fingerprint, :created_date, :datasource, :realtime, :payload)",
                  schemaTable
              )
          );

          List<SegmentSchema> schemaList = Lists.newArrayList(
              new SegmentSchema("schema_id_1",
                                RowSignature.builder()
                                            .add("dim1", ColumnType.LONG)
                                            .build()),
              new SegmentSchema("schema_id_2",
                                RowSignature.builder()
                                            .add("dim1", ColumnType.LONG)
                                            .add("dim2", ColumnType.LONG)
                                            .build())
          );

          for (SegmentSchema schema : schemaList) {
            preparedBatch1.add()
                .bind("fingerprint", schema.getFingerprint())
                .bind("created_date", DateTimes.nowUtc().toString())
                .bind("datasource", "fooDataSource")
                .bind("realtime", false)
                .bind("payload", mapper.writeValueAsBytes(schema));
          }

          final int[] affectedRows1 = preparedBatch1.execute();
          final boolean succeeded1 = Arrays.stream(affectedRows1).allMatch(eachAffectedRows -> eachAffectedRows == 1);
          if (!succeeded1) {
            throw new ISE("Failed to publish segments schema to DB");
          }
          return true;
        }
    );
  }
}
