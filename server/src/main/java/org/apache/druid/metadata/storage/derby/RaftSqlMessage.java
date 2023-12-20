package org.apache.druid.metadata.storage.derby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RaftSqlMessage
{
  @JsonProperty
  byte[] message;

  @JsonProperty
  String type;

  @JsonCreator
  public RaftSqlMessage(
      @JsonProperty("message") byte[] message,
      @JsonProperty("type") String type)
  {
    this.message = message;
    this.type = type;
  }

  @JsonProperty
  public byte[] getMessage()
  {
    return message;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }
}
