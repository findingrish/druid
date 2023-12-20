package org.apache.druid.metadata.storage.derby;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.nio.charset.StandardCharsets;

public class RaftLogMessage implements Message
{

  private final String sql;

  public RaftLogMessage(String sql)
  {
    this.sql = sql;
  }

  public RaftLogMessage(ByteString byteString)
  {
    this.sql = new String(byteString.toByteArray(), StandardCharsets.UTF_8);
  }

  public String getSql()
  {
    return sql;
  }

  @Override
  public ByteString getContent()
  {
    return toByteString(sql.getBytes(StandardCharsets.UTF_8));
  }

  static ByteString toByteString(byte[] bytes) {
    return toByteString(bytes, 0, bytes.length);
  }

  static ByteString toByteString(byte[] bytes, int offset, int size) {
    // return singleton to reduce object allocation
    return bytes.length == 0 ?
           ByteString.EMPTY : ByteString.copyFrom(bytes, offset, size);
  }

}
