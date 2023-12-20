package org.apache.druid.metadata.storage.derby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RaftBindingValue
{
  @JsonProperty
  Object value;

  @JsonProperty
  Class<?> type;

  @JsonCreator
  public RaftBindingValue(
      @JsonProperty("value") Object value,
      @JsonProperty("type") Class<?> type)
  {
    this.value = value;
    this.type = type;
  }

  @JsonProperty
  public Object getValue()
  {
    return value;
  }

  @JsonProperty
  public Class<?> getType()
  {
    return type;
  }

  @Override
  public String toString()
  {
    return "RaftBindingValue{" +
           "value=" + value +
           ", type=" + type +
           '}';
  }
}
