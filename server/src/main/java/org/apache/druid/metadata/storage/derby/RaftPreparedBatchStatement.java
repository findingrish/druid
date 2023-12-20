package org.apache.druid.metadata.storage.derby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class RaftPreparedBatchStatement
{
  @JsonProperty
  String sql;
  @JsonProperty
  List<RaftPreparedBatchStatementPart> parts;

  @JsonCreator
  public RaftPreparedBatchStatement(
      @JsonProperty("sql") String sql,
      @JsonProperty("parts") List<RaftPreparedBatchStatementPart> parts)
  {
    this.sql = sql;
    this.parts = parts;
  }

  public RaftPreparedBatchStatement(String sql)
  {
    this.sql = sql;
    this.parts = new ArrayList<>();
  }

  public RaftPreparedBatchStatement add(RaftPreparedBatchStatementPart part) {
    parts.add(part);
    return this;
  }

  @JsonProperty
  public String getSql()
  {
    return sql;
  }

  @JsonProperty
  public List<RaftPreparedBatchStatementPart> getParts()
  {
    return parts;
  }

  @Override
  public String toString()
  {
    return "RaftPreparedBatchStatement{" +
           "sql='" + sql + '\'' +
           ", parts=" + parts +
           '}';
  }
}
