package org.apache.druid.metadata.storage.derby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class RaftPreparedBatchStatementPart
{
  @JsonProperty
  private Map<String, RaftBindingValue> bindings;

  public RaftPreparedBatchStatementPart()
  {
    this.bindings = new HashMap<>();
  }

  @JsonCreator
  public RaftPreparedBatchStatementPart(
      @JsonProperty("bindings") Map<String, RaftBindingValue> bindings) {
    this.bindings = bindings;
  }

  public RaftPreparedBatchStatementPart bind(String name, String value) {
    bindings.put(name, new RaftBindingValue(value, String.class));
    return this;
  }

  public RaftPreparedBatchStatementPart bind(String name, boolean value) {
    bindings.put(name, new RaftBindingValue(value, Boolean.class));
    return this;
  }

  public RaftPreparedBatchStatementPart bind(String name, byte[] value) {
    bindings.put(name, new RaftBindingValue(value, Byte.class));
    return this;
  }

  @JsonProperty
  public Map<String, RaftBindingValue> getBindings()
  {
    return bindings;
  }

  @Override
  public String toString()
  {
    return "RaftPreparedBatchStatementPart{" +
           "bindings=" + bindings +
           '}';
  }
}
