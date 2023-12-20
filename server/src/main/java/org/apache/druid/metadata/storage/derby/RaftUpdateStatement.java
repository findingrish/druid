package org.apache.druid.metadata.storage.derby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class RaftUpdateStatement
{
  private String sql;
  private Map<String, RaftBindingValue> bindings;

  public RaftUpdateStatement(String sql)
  {
    this.sql = sql;
    this.bindings = new HashMap<>();
  }

  @JsonCreator
  public RaftUpdateStatement(
      @JsonProperty("sql") String sql,
      @JsonProperty("bindings") Map<String, RaftBindingValue> bindings) {
    this.sql = sql;
    this.bindings = bindings;
  }

  public RaftUpdateStatement bind(String name, String value) {
    bindings.put(name, new RaftBindingValue(value, String.class));
    return this;
  }

  public RaftUpdateStatement bind(String name, boolean value) {
    bindings.put(name, new RaftBindingValue(value, Boolean.class));
    return this;
  }

  public RaftUpdateStatement bind(String name, byte[] value) {
    bindings.put(name, new RaftBindingValue(value, Byte.class));
    return this;
  }

  @JsonProperty
  public String getSql()
  {
    return sql;
  }

  @JsonProperty
  public Map<String, RaftBindingValue> getBindings()
  {
    return bindings;
  }

  @Override
  public String toString()
  {
    return "RaftUpdateStatement{" +
           "sql='" + sql + '\'' +
           ", bindings=" + bindings +
           '}';
  }
}
