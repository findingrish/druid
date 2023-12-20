package org.apache.druid.metadata.storage.derby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class RaftClient implements Closeable
{
  private Logger log = new Logger(RaftClient.class);

  public static final List<RaftPeer> PEERS = ImmutableList.of(
      RaftPeer.newBuilder().setId("n0").setAddress("127.0.0.1:9081").setPriority(0).build(),
      RaftPeer.newBuilder().setId("n1").setAddress("127.0.0.1:9095").setPriority(0).build()
  );

  static final UUID GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");

  static final RaftGroup RAFT_GROUP = RaftGroup.valueOf(RaftGroupId.valueOf(GROUP_ID), PEERS);

  ObjectMapper objectMapper;

  @Inject
  public RaftClient(ObjectMapper mapper)
  {
    this.objectMapper = mapper;
  }

  //build the client
  static org.apache.ratis.client.RaftClient newClient() {
    return org.apache.ratis.client.RaftClient.newBuilder()
                                             .setProperties(new RaftProperties())
                                             .setRaftGroup(RAFT_GROUP)
                                             .build();
  }

  private final org.apache.ratis.client.RaftClient client = newClient();

  @Override
  public void close() throws IOException {
    client.close();
  }

  public void send(RaftUpdateStatement updateStatement)
  {

    final RaftClientReply reply;
    try {
      byte[] bytes =  objectMapper.writeValueAsBytes(updateStatement);
      RaftSqlMessage raftSqlMessage = new RaftSqlMessage(bytes, "0");
      reply = client.io().send(Message.valueOf(ByteString.copyFrom(objectMapper.writeValueAsBytes(raftSqlMessage))));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    log.info("Request sent. isSuccess[%s]", reply.isSuccess());
  }

  public void send(RaftPreparedBatchStatement updateStatement)
  {

    final RaftClientReply reply;
    try {
      byte[] bytes =  objectMapper.writeValueAsBytes(updateStatement);
      RaftSqlMessage raftSqlMessage = new RaftSqlMessage(bytes, "1");
      reply = client.io().send(Message.valueOf(ByteString.copyFrom(objectMapper.writeValueAsBytes(raftSqlMessage))));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    log.info("Request sent. isSuccess[%s]", reply.isSuccess());
  }
}
