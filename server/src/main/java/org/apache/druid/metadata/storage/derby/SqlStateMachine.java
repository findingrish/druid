package org.apache.druid.metadata.storage.derby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MD5FileUtil;
import org.eclipse.aether.spi.log.LoggerFactory;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.PreparedBatchPart;
import org.skife.jdbi.v2.Update;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SqlStateMachine extends BaseStateMachine
{

  private static final Logger log = new Logger(SqlStateMachine.class);
  private final ObjectMapper jsonMapper;

  private SQLMetadataConnector connector;
  private List<String> sql = new LinkedList<>();
  private SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  void reset() {
    sql.clear();
    setLastAppliedTermIndex(null);
  }

  @Inject
  public SqlStateMachine(
      SQLMetadataConnector connector,
      ObjectMapper jsonMapper
  )
  {
    this.connector = connector;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void initialize(
      RaftServer server, RaftGroupId groupId,
      RaftStorage raftStorage) throws IOException
  {
    super.initialize(server, groupId, raftStorage);
    this.storage.init(raftStorage);
    loadSnapshot(storage.getLatestSnapshot());
  }

  @Override
  public void reinitialize() throws IOException {
    close();
    loadSnapshot(storage.getLatestSnapshot());
  }

  @Override
  public long takeSnapshot() {
    log.info("StateMachine takeSnapshot is called.");
    final List<String> copy;
    final TermIndex last;
    try(AutoCloseableLock readLock = readLock()) {
      copy = new LinkedList<>(sql);
      last = getLastAppliedTermIndex();
    }

    final File snapshotFile =  storage.getSnapshotFile(last.getTerm(), last.getIndex());
    log.info("Taking a snapshot to file {%s}", snapshotFile);

    try(ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(
        FileUtils.newOutputStream(snapshotFile)))) {
      out.writeObject(copy);
    } catch(IOException ioe) {
      log.info("Failed to write snapshot file \"" + snapshotFile
               + "\", last applied index=" + last);
    }

    final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
    final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
    storage.updateLatestSnapshot(new SingleFileSnapshotInfo(info, last));
    return last.getIndex();
  }

  public long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    log.info("StateMachine loadSnapshot is called.");
    if (snapshot == null) {
      log.info("The snapshot info is null.");
      return RaftLog.INVALID_LOG_INDEX;
    }
    final File snapshotFile = snapshot.getFile().getPath().toFile();
    if (!snapshotFile.exists()) {
      log.info("The snapshot file {%s} does not exist for snapshot {%s}", snapshotFile, snapshot);
      return RaftLog.INVALID_LOG_INDEX;
    }

    // verify md5
    final MD5Hash md5 = snapshot.getFile().getFileDigest();
    if (md5 != null) {
      MD5FileUtil.verifySavedMD5(snapshotFile, md5);
    }

    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    try(AutoCloseableLock writeLock = writeLock();
        ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(
            FileUtils.newInputStream(snapshotFile)))) {
      reset();
      setLastAppliedTermIndex(last);
      List<String> temp = new LinkedList<>(JavaUtils.cast(in.readObject()));
      for (String t : temp) {
        connector.retryWithHandle(v -> v.createStatement(t).execute());
        sql.add(t);
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Failed to load " + snapshot, e);
    }
    return last.getIndex();
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }


  @Override
  public void close() {
    reset();
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final RaftProtos.LogEntryProto entry = trx.getLogEntry();
    ByteString byteString = entry.getStateMachineLogEntry().getLogData();

    RaftUpdateStatement raftUpdateStatement = null;
    RaftPreparedBatchStatement preparedBatchStatement = null;
    String type = null;
    try {
      RaftSqlMessage sqlMessage = jsonMapper.readValue(byteString.toByteArray(), RaftSqlMessage.class);
      type = sqlMessage.getType();
      if (sqlMessage.getType().equals("0")) {
        raftUpdateStatement = jsonMapper.readValue(sqlMessage.getMessage(), RaftUpdateStatement.class);
        log.info("Received update statement. [%s]", raftUpdateStatement);
        RaftUpdateStatement finalRaftUpdateStatement = raftUpdateStatement;
        connector.retryWithHandle(handle -> {
          Update update = handle.createStatement(finalRaftUpdateStatement.getSql());
          for (Map.Entry<String, RaftBindingValue> binding : finalRaftUpdateStatement.getBindings().entrySet()) {
            RaftBindingValue value = binding.getValue();
            if (value.getType() == String.class) {
              String v = (String) value.getValue();
              update.bind(binding.getKey(), v);
            } else if (value.getType() == Byte.class) {
              byte[] decoded = Base64.getDecoder().decode(((String)value.getValue()).getBytes());
              update.bind(binding.getKey(), decoded);
            } else if (value.getType() == Boolean.class) {
              update.bind(binding.getKey(), value.getValue());
            } else {
              throw new RuntimeException("Received binding of unknown type.");
            }
          }
          return update.execute();
        });
      } else if (sqlMessage.getType().equals("1")) {
        preparedBatchStatement = jsonMapper.readValue(sqlMessage.getMessage(), RaftPreparedBatchStatement.class);
        log.info("Received prepared batch statement %s", preparedBatchStatement);

        RaftPreparedBatchStatement finalPreparedBatchStatement = preparedBatchStatement;
        connector.retryWithHandle(handle -> {
          PreparedBatch batch = handle.prepareBatch(finalPreparedBatchStatement.getSql());
          for (RaftPreparedBatchStatementPart part : finalPreparedBatchStatement.getParts()) {
            PreparedBatchPart preparedBatchPart = batch.add();
            for (Map.Entry<String, RaftBindingValue> binding : part.getBindings().entrySet()) {
              RaftBindingValue value = binding.getValue();
              if (value.getType() == String.class) {
                String v = (String) value.getValue();
                preparedBatchPart.bind(binding.getKey(), v);
              } else if (value.getType() == Byte.class) {
                byte[] decoded = Base64.getDecoder().decode(((String)value.getValue()).getBytes());
                preparedBatchPart.bind(binding.getKey(), decoded);
              } else if (value.getType() == Boolean.class) {
                preparedBatchPart.bind(binding.getKey(), value.getValue());
              } else {
                throw new RuntimeException("Received binding of unknown type.");
              }
            }
          }
          return batch.execute();
        });
      } else {
        throw new RuntimeException(StringUtils.format("RaftSqlMessage of unknown type [%s]", sqlMessage.getType()));
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    final long index = entry.getIndex();
    try(AutoCloseableLock writeLock = writeLock()) {
      //connector.retryWithHandle(handle -> handle.createStatement(sql).execute());
      updateLastAppliedTermIndex(entry.getTerm(), index);
    }
    final CompletableFuture<Message> f = CompletableFuture.completedFuture(Message.valueOf("success"));

    final RaftProtos.RaftPeerRole role = trx.getServerRole();
    if (role == RaftProtos.RaftPeerRole.LEADER) {
      log.info("{%s}:{%s}-{%s}: = {%s}", role, getId(), index, sql);
    } else {
      log.info("{%s}:{%s}-{%s}: = {%s}", role, getId(), index, sql);
    }
    return f;
  }
}
