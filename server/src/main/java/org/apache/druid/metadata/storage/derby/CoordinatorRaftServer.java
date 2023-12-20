package org.apache.druid.metadata.storage.derby;

import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TimeDuration;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

@ManageLifecycle
public class CoordinatorRaftServer implements Closeable
{

  private static final Logger log = new Logger(CoordinatorRaftServer.class);
  private final SqlStateMachine sqlStateMachine;
  private final RaftServer server;

  private final ScheduledExecutorService executorService;

  @Inject
  public CoordinatorRaftServer(SqlStateMachine sqlStateMachine, RaftConfig config, ScheduledExecutorFactory executorFactory) throws IOException {

    log.info("Initialising CooordinatorRaft Server, peerIndex is [%s]", config.getPeerIndex());

    this.sqlStateMachine = sqlStateMachine;
    //create a property object
    final RaftProperties properties = new RaftProperties();

    final RaftPeer peer = RaftClient.PEERS.get(config.getPeerIndex());
    final File storageDir = new File("./" + peer.getId());

    //set the storage directory (different for each peer) in the RaftProperty object
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    //set the read policy to Linearizable Read.
    //the Default policy will route read-only requests to leader and directly query leader statemachine.
    //Linearizable Read allows to route read-only requests to any group member
    //and uses ReadIndex to guarantee strong consistency.
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    //set the linearizable read timeout
    RaftServerConfigKeys.Read.setTimeout(properties, TimeDuration.ONE_MINUTE);

    //set the port (different for each peer) in RaftProperty object
    final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    //build the Raft server
    this.server = RaftServer.newBuilder()
                            .setGroup(RaftClient.RAFT_GROUP)
                            .setProperties(properties)
                            .setServerId(peer.getId())
                            .setStateMachine(sqlStateMachine)
                            .setOption(RaftStorage.StartupOption.RECOVER)
                            .build();

    executorService = executorFactory.create(1, "RaftLeaderPollTask-%d");
  }

  @LifecycleStart
  public void start() throws IOException {
    log.info("Starting Coordinator raft server.");
    server.start();
    executorService.schedule(() -> {
      try {
        log.info("Running raft leadership thread. Am I the leader [%s], current role [%s], current term [%s], leader id [%s]",
                 server.getDivision(RaftClient.RAFT_GROUP.getGroupId()).getInfo().isLeader(),
                 server.getDivision(RaftClient.RAFT_GROUP.getGroupId()).getInfo().getCurrentRole(),
                 server.getDivision(RaftClient.RAFT_GROUP.getGroupId()).getInfo().getCurrentTerm(),
                 server.getDivision(RaftClient.RAFT_GROUP.getGroupId()).getInfo().getLeaderId()
                 );
      }
      catch (IOException e) {
        log.error("Error fetching current leader.");
      }
    }, 10, TimeUnit.SECONDS);
  }

  @LifecycleStop
  @Override
  public void close() throws IOException {
    log.info("Stopping Coordinator raft server.");
    server.close();
  }

  public RaftServer getServer()
  {
    return server;
  }


}
