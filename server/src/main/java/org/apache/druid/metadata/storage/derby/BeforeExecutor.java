package org.apache.druid.metadata.storage.derby;

import org.apache.druid.java.util.common.logger.Logger;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.SQLLog;

import java.sql.PreparedStatement;
import java.sql.Statement;

public class BeforeExecutor implements SQLLog
{
  private RaftClient raftClient;
  private Logger log = new Logger(BeforeExecutor.class);

  public BeforeExecutor()
  {
    this.raftClient = null;
  }

  @Override
  public void logBeginTransaction(Handle h)
  {
    log.info("[Custom Logging] Method logBeginTransaction.");
  }

  @Override
  public void logCommitTransaction(long time, Handle h)
  {
    log.info("[Custom Logging] Method logCommitTransaction.");
  }

  @Override
  public void logRollbackTransaction(long time, Handle h)
  {
    log.info("[Custom Logging] Method logRollbackTransaction.");
  }

  @Override
  public void logObtainHandle(long time, Handle h)
  {
    log.info("[Custom Logging] Method logObtainHandle.");
  }

  @Override
  public void logReleaseHandle(Handle h)
  {
    log.info("[Custom Logging] Method logReleaseHandle.");
  }

  @Override
  public void logSQL(long time, String sql)
  {
    if (!(sql.startsWith("CREATE TABLE") || sql.startsWith("CREATE INDEX") || sql.startsWith("SELECT") || sql.startsWith("select"))) {
     // raftClient.send(sql);
    }
    log.info("[Custom Logging] Method logSQL. Sql is [%s]", sql);
  }

  @Override
  public void beforeExecute(PreparedStatement stmt, String sql)
  {
    log.info("[Custom Logging] Method beforeExecute. Sql is [%s], Prepared stmt is [%s], stmt class [%s]", sql, stmt.toString(), stmt.getClass());
  }

  @Override
  public void logPreparedBatch(long time, String sql, int count)
  {
   // raftClient.send(sql);
    log.info("[Custom Logging] Method logPreparedBatch. Sql is [%s]", sql);
  }

  @Override
  public void beforePreparedBatch(PreparedStatement stmt)
  {
    log.info("[Custom Logging] Method beforePreparedBatch. Prepared stmt is [%s], stmt class [%s]", stmt.toString(), stmt.getClass());
  }

  @Override
  public BatchLogger logBatch()
  {
    log.info("[Custom Logging] Method logBatch.");
    final StringBuilder builder = new StringBuilder();
    builder.append("batch:[");
    return new BatchLogger()
    {
      private boolean added = false;

      @Override
      public void add(String sql)
      {
        added = true;
        builder.append("[").append(sql).append("], ");
      }

      @Override
      public void log(long time)
      {
        if (added) {
          builder.delete(builder.length() - 2, builder.length());
        }
        builder.append("]");
        log.info(String.format("%s took %d millis", builder.toString(), time));
      }
    };
  }

  @Override
  public void beforeBatch(Statement stmt)
  {
    log.info("[Custom Logging] Method beforeBatch. stmt is [%s], stmt class [%s]", stmt.toString(), stmt.getClass());
  }

  @Override
  public void logCheckpointTransaction(Handle h, String name)
  {
    log.info("[Custom Logging] Method logCheckpointTransaction.");
  }

  @Override
  public void logReleaseCheckpointTransaction(Handle h, String name)
  {
    log.info("[Custom Logging] Method logReleaseCheckpointTransaction.");
  }

  @Override
  public void logRollbackToCheckpoint(long time, Handle h, String checkpointName)
  {
    log.info("[Custom Logging] Method logRollbackToCheckpoint.");
  }
}
