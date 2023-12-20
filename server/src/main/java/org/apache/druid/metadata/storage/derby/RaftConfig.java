package org.apache.druid.metadata.storage.derby;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RaftConfig
{
  @JsonProperty
  boolean enable;
  @JsonProperty
  int peerIndex;

  public int getPeerIndex()
  {
    return peerIndex;
  }

  public boolean isEnable()
  {
    return enable;
  }
}
