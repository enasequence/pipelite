/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.storage;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import pipelite.process.state.ProcessExecutionState;
import pipelite.repository.PipeliteProcessRepository;
import pipelite.task.result.TaskExecutionResult;
import pipelite.task.instance.LatestTaskExecution;
import pipelite.task.instance.TaskInstance;
import pipelite.task.result.TaskExecutionResultType;

public class OracleStorage implements OracleCommons, StorageBackend {

  private static final String LOCK_EXPRESSION = "for update nowait";

  @Deprecated private String log_table_name;

  private static final String ENABLED_VALUE = "Y";

  private Connection connection;
  private String pipeline_name;

  public OracleStorage() {
  }


  @Override
  public void load(LatestTaskExecution instance) throws StorageException {
    PreparedStatement ps = null;
    ResultSet rows = null;
    try {
      ps =
          connection.prepareStatement(
              String.format(
                  "select %s, %s, %s, %s, %s, %s, %s, %s " + "  from %s" + " where %s = ?",
                  EXEC_ID_COLUMN_NAME,
                  EXEC_DATE_COLUMN_NAME,
                  EXEC_START_COLUMN_NAME,
                  EXEC_RESULT_COLUMN_NAME,
                  EXEC_RESULT_TYPE_COLUMN_NAME,

                  // TODO remove
                  EXEC_STDERR_COLUMN_NAME,
                  EXEC_STDOUT_COLUMN_NAME,
                  EXEC_CMDLINE_COLUMN_NAME,
                  PIPELINE_STAGE_TABLE_NAME,
                  EXEC_ID_COLUMN_NAME));

      ps.setObject(1, instance.getExecutionId());

      rows = ps.executeQuery();
      int rownum = 0;
      while (rows.next()) {
        instance.setEndTime(rows.getTimestamp(EXEC_DATE_COLUMN_NAME));
        instance.setStartTime(rows.getTimestamp(EXEC_START_COLUMN_NAME));

        String resultName = rows.getString(EXEC_RESULT_COLUMN_NAME);
        String resultTypeString = rows.getString(EXEC_RESULT_TYPE_COLUMN_NAME);
        TaskExecutionResultType resultType =
            resultTypeString == null ? null : TaskExecutionResultType.valueOf(resultTypeString);
        instance.setTaskExecutionResult(new TaskExecutionResult(resultName, resultType));

        // TODO remove
        instance.setStderr(rows.getString(EXEC_STDERR_COLUMN_NAME));
        instance.setStdout(rows.getString(EXEC_STDOUT_COLUMN_NAME));
        instance.setCmd(rows.getString(EXEC_CMDLINE_COLUMN_NAME));

        ++rownum;
      }
      if (1 != rownum)
        throw new StorageException(
            String.format(
                "Failed to load execution instance [%s] of [%s] pipeline",
                instance.getExecutionId(), pipeline_name));
    } catch (SQLException e) {
      throw new StorageException(e);

    } finally {
      if (null != rows) {
        try {
          rows.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }

      if (null != ps) {
        try {
          ps.close();
        } catch (SQLException e) {
        }
      }
    }
  }

  @Deprecated
  public void load(TaskInstance instance, boolean lock) throws StorageException {
    PreparedStatement ps = null;
    ResultSet rows = null;

    try {
      ps =
          connection.prepareStatement(
              String.format(
                  "select %s, %s, %s from %s" + " where %s = ? and %s = ? and %s = ? %s",
                  ATTEMPT_COLUMN_NAME,
                  ENABLED_COLUMN_NAME,
                  EXEC_ID_COLUMN_NAME,
                  PIPELINE_STAGE_TABLE_NAME,
                  PIPELINE_COLUMN_NAME,
                  PROCESS_COLUMN_NAME,
                  STAGE_NAME_COLUMN_NAME,
                  lock ? LOCK_EXPRESSION : ""));

      ps.setObject(1, pipeline_name);
      ps.setObject(2, instance.getProcessId());
      ps.setString(3, instance.getTaskName());

      rows = ps.executeQuery();

      int rownum = 0;
      while (rows.next()) {
        instance.getLatestTaskExecution().setExecutionId(rows.getString(EXEC_ID_COLUMN_NAME));
        instance.setExecutionCount((int) rows.getLong(ATTEMPT_COLUMN_NAME));
        instance.setEnabled(ENABLED_VALUE.equals(rows.getString(ENABLED_COLUMN_NAME)));
        ++rownum;
      }

      if (1 != rownum)
        throw new StorageException(
            String.format(
                "Failed to load stage [%s] for process [%s] of [%s] pipeline",
                instance.getTaskName(), instance.getProcessId(), pipeline_name));

      if (null != instance.getLatestTaskExecution().getExecutionId())
        load(instance.getLatestTaskExecution());

    } catch (SQLException e) {
      throw new StorageException(e);

    } finally {
      if (null != rows) {
        try {
          rows.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }

      if (null != ps) {
        try {
          ps.close();
        } catch (SQLException e) {
        }
      }
    }
  }

  @Override
  public void load(TaskInstance instance) throws StorageException {
    load(instance, false);
  }

  @Override
  public void save(TaskInstance instance) throws StorageException {
    List<Clob> clob_list = new ArrayList<>();
    PreparedStatement ps = null;
    try {
      String sql =
          String.format(
              " merge into %14$s T0 "
                  + " using ( select ? %1$s, ? %2$s, ? %3$s, ? %4$s, ? %5$s, ? %6$s, ? %7$s, ? %8$s, ? %9$s, ? %10$s, ? %11$s, ? %12$s, ? %13$s from dual ) T1 "
                  + " on ( T0.%1$s = T1.%1$s and T0.%2$s = T1.%2$s and T0.%3$s = T1.%3$s ) "
                  + " when matched then update set "
                  + "  %4$s = T1.%4$s, "
                  + "  %5$s = T1.%5$s, "
                  + "  %6$s = T1.%6$s, "
                  + "  %7$s = T1.%7$s, "
                  + "  %8$s = T1.%8$s, "
                  + "  %9$s = T1.%9$s, "
                  + "  %10$s = T1.%10$s, "
                  + "  %11$s = T1.%11$s, "
                  + "  %12$s = T1.%12$s, "
                  + "  %13$s = T1.%13$s "
                  + " when not matched then insert( %1$s, %2$s, %3$s, %4$s, %5$s, %6$s, %7$s, %8$s, %9$s, %10$s, %11$s, %12$s, %13$s ) "
                  + "                       values( T1.%1$s, T1.%2$s, T1.%3$s, T1.%4$s, T1.%5$s, T1.%6$s, T1.%7$s, T1.%8$s, T1.%9$s, T1.%10$s, T1.%11$s, T1.%12$s, T1.%13$s ) ",
              /* 1 */ PIPELINE_COLUMN_NAME,
              /* 2 */ PROCESS_COLUMN_NAME,
              /* 3 */ STAGE_NAME_COLUMN_NAME,
              /* 4 */ ATTEMPT_COLUMN_NAME,
              /* 5 */ EXEC_ID_COLUMN_NAME,
              /* 6 */ ENABLED_COLUMN_NAME,
              /* 7 */ EXEC_START_COLUMN_NAME,
              /* 8 */ EXEC_DATE_COLUMN_NAME,
              /* 9 */ EXEC_RESULT_TYPE_COLUMN_NAME,
              /* 10 */ EXEC_RESULT_COLUMN_NAME,
              /* 11 */ EXEC_STDOUT_COLUMN_NAME,
              /* 12 */ EXEC_STDERR_COLUMN_NAME,
              /* 13 */ EXEC_CMDLINE_COLUMN_NAME,
              /* 14 */ PIPELINE_STAGE_TABLE_NAME);

      ps = connection.prepareStatement(sql);

      ps.setObject(1, pipeline_name);
      ps.setObject(2, instance.getProcessId());
      ps.setString(3, instance.getTaskName());

      ps.setObject(4, instance.getExecutionCount());
      ps.setObject(
          5,
          null == instance.getLatestTaskExecution()
              ? null
              : instance.getLatestTaskExecution().getExecutionId());
      ps.setObject(6, instance.isEnabled() ? "Y" : "N");

      ps.setObject(
          7,
          null == instance.getLatestTaskExecution()
              ? null
              : instance.getLatestTaskExecution().getStartTime());
      ps.setObject(
          8,
          null == instance.getLatestTaskExecution()
              ? null
              : instance.getLatestTaskExecution().getEndTime());
      ps.setString(
          9,
          null == instance.getLatestTaskExecution()
              ? null
              : null == instance.getLatestTaskExecution().getResultType()
                  ? null
                  : instance.getLatestTaskExecution().getResultType().toString());

      ps.setObject(
          10,
          null == instance.getLatestTaskExecution()
              ? null
              : instance.getLatestTaskExecution().getResultName());

      ps.setObject(
          11,
          makeClob(
              connection,
              null == instance.getLatestTaskExecution()
                  ? null
                  : instance.getLatestTaskExecution().getStdout(),
              clob_list));
      ps.setObject(
          12,
          makeClob(
              connection,
              null == instance.getLatestTaskExecution()
                  ? null
                  : instance.getLatestTaskExecution().getStderr(),
              clob_list));
      ps.setObject(
          13,
          makeClob(
              connection,
              null == instance.getLatestTaskExecution()
                  ? null
                  : instance.getLatestTaskExecution().getCmd(),
              clob_list));

      int rows = ps.executeUpdate();
      if (1 != rows) throw new StorageException("Can't update exactly 1 row!");
      /*
                  if( null != instance.getExecutionInstance().getExecutionId() )
                      save( instance.getExecutionInstance() );
      */
    } catch (SQLException e) {
      throw new StorageException(e);

    } finally {
      clob_list.forEach(
          e -> {
            try {
              e.free();
            } catch (SQLException e1) {
              throw new RuntimeException(e1);
            }
          });

      if (null != ps) {
        try {
          ps.close();
        } catch (SQLException e) {
        }
      }
    }
  }

  private Clob makeClob(Connection connection, String value, List<Clob> list) throws SQLException {
    Clob clob = connection.createClob();
    clob.setString(1, value);
    list.add(clob);
    return clob;
  }

  // TODO add exec_id
  @Override
  public void save(ProcessLogBean bean) throws StorageException {
    PreparedStatement ps = null;
    try {
      ps =
          connection.prepareStatement(
              String.format(
                  "DECLARE "
                      + " PRAGMA AUTONOMOUS_TRANSACTION; "
                      + "BEGIN"
                      + " insert into %s ( %s, %s, %s, %s, LOG_DATE, MESSAGE, EXCEPTION, JOBID, HOSTS ) "
                      + " values ( ?, ?, ?, ?, sysdate,  ?, ?, ?, ? );"
                      + " IF 1 <> sql%%rowcount THEN "
                      + "  ROLLBACK;"
                      + "  raise_application_error( -20000, 'Insert into %s failed' ); "
                      + " END IF;"
                      + "COMMIT; "
                      + "END;",
                  log_table_name,
                  PIPELINE_COLUMN_NAME,
                  PROCESS_COLUMN_NAME,
                  STAGE_NAME_COLUMN_NAME,
                  EXEC_ID_COLUMN_NAME,
                  log_table_name));
      ps.setString(1, bean.getPipelineName());
      ps.setString(2, bean.getProcessId());
      ps.setString(3, bean.getStage());
      ps.setObject(4, bean.getExecutionId());
      ps.setString(5, bean.getMessage());
      ps.setString(
          6,
          null != bean.getExceptionText() && bean.getExceptionText().length() > 3000
              ? bean.getExceptionText().substring(0, 3000)
              : bean.getExceptionText());
      ps.setObject(7, bean.getLSFJobID());
      ps.setString(8, bean.getLSFHost());

      int rows = ps.executeUpdate();
      if (1 != rows)
        throw new StorageException(String.format("Unable to insert into %s", log_table_name));

    } catch (SQLException | NoSuchFieldException e) {
      throw new StorageException(e);

    } finally {
      if (null != ps) {
        try {
          ps.close();
        } catch (SQLException e) {
        }
      }
    }
  }

  @Override
  public void flush() throws StorageException {
    try {
      connection.commit();
    } catch (SQLException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void close() throws StorageException {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new StorageException(e);
    }
  }

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  public String getPipelineName() {
    return pipeline_name;
  }

  public void setPipelineName(String pipeline_name) {
    this.pipeline_name = pipeline_name;
  }

  @Override
  public void save(LatestTaskExecution instance) throws StorageException {
    List<Clob> clob_list = new ArrayList<>();
    PreparedStatement ps = null;
    try {
      ps =
          connection.prepareStatement(
              String.format(
                  "update %s "
                      + "   set %s = ?, %s = ?, %s = ?, "
                      + "       %s = ?, %s = ?, %s = ?, %s = ? "
                      + " where %s = ?",
                  PIPELINE_STAGE_TABLE_NAME,
                  EXEC_START_COLUMN_NAME,
                  EXEC_DATE_COLUMN_NAME,
                  EXEC_RESULT_TYPE_COLUMN_NAME,
                  EXEC_RESULT_COLUMN_NAME,
                  EXEC_STDERR_COLUMN_NAME,
                  EXEC_STDOUT_COLUMN_NAME,
                  EXEC_CMDLINE_COLUMN_NAME,
                  EXEC_ID_COLUMN_NAME));

      ps.setObject(1, instance.getStartTime());
      ps.setObject(2, instance.getEndTime());
      ps.setString(
          3, null == instance.getResultType() ? null : instance.getResultType().toString());

      ps.setObject(4, instance.getResultName());

      ps.setObject(5, makeClob(connection, instance.getStderr(), clob_list));
      ps.setObject(6, makeClob(connection, instance.getStdout(), clob_list));
      ps.setObject(7, makeClob(connection, instance.getCmd(), clob_list));

      ps.setString(8, instance.getExecutionId());

      int rows = ps.executeUpdate();
      if (1 != rows) throw new StorageException("Can't update exactly 1 row!");

    } catch (SQLException e) {
      throw new StorageException(e);

    } finally {
      clob_list.forEach(
          e -> {
            try {
              e.free();
            } catch (SQLException e1) {
              throw new RuntimeException(e1);
            }
          });

      if (null != ps) {
        try {
          ps.close();
        } catch (SQLException e) {
        }
      }
    }
  }

  @Override
  public String getExecutionId() throws StorageException {

    try (PreparedStatement ps =
        connection.prepareStatement(
            String.format("select %s.nextVal from dual", EXEC_ID_SEQUENCE))) {
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) return rs.getString(1);
      }
    } catch (SQLException e) {
      throw new StorageException(e);
    }
    return null;
  }
}
