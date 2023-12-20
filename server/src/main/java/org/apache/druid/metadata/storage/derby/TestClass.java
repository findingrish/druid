package org.apache.druid.metadata.storage.derby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Base64;

public class TestClass
{

  public static void main(String[] args) throws IOException
  {

    ObjectMapper objectMapper = new ObjectMapper();
    RaftUpdateStatement raftUpdateStatement = new RaftUpdateStatement("select from this table");
    raftUpdateStatement.bind("id", "xyz")
                       .bind("payload", objectMapper.writeValueAsBytes(new TestObj(1, "w")))
        .bind("boolean", false);

    byte[] bytes =  objectMapper.writeValueAsBytes(raftUpdateStatement);
    RaftSqlMessage raftSqlMessage = new RaftSqlMessage(bytes, "1");

    byte[] messageBytes = objectMapper.writeValueAsBytes(raftSqlMessage);

    RaftSqlMessage deserializedMessage = objectMapper.readValue(messageBytes, RaftSqlMessage.class);
    RaftUpdateStatement w = objectMapper.readValue(deserializedMessage.getMessage(), RaftUpdateStatement.class);
    byte[] x = Base64.getDecoder().decode(((String)w.getBindings().get("payload").getValue()).getBytes());
    TestObj p = objectMapper.readValue(x, TestObj.class);
    int t = 1;

  }

  private static class TestObj {
    @JsonProperty
    int x;

    @JsonProperty
    String y;

    @JsonCreator
    public TestObj(
        @JsonProperty("x") int x,
        @JsonProperty("y") String y)
    {
      this.x = x;
      this.y = y;
    }

    @JsonProperty
    public int getX()
    {
      return x;
    }

    @JsonProperty
    public String getY()
    {
      return y;
    }
  }
}
