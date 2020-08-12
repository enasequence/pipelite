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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.TaskIdSource;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;

public class OracleTaskIdSource2 implements OracleCommons, TaskIdSource {
  Logger log = Logger.getLogger(this.getClass());
  private PreparedStatement selectPS;
  private String table_name;
  private String pipeline_name;
  private int redo_count;
  private ExecutionResult[] commit_status;
  private Connection connection;

  private String prepareQuery() {
    StringBuilder redo = new StringBuilder();
    StringBuilder terminal = new StringBuilder();

    for (ExecutionResult cs : commit_status) {
      if (cs.getType().canReprocess()) redo.append("'").append(cs.toString()).append("', ");

      if (cs.getType().isFailure() && !cs.getType().canReprocess())
        terminal.append("'").append(cs.toString()).append("', ");
    }

    if (redo.length() <= 2) redo.delete(0, redo.length()).append("''");
    else redo.setLength(redo.length() - 2);

    if (terminal.length() <= 2) terminal.delete(0, terminal.length()).append("''");
    else terminal.setLength(terminal.length() - 2);

    /*
            with T1 as ( select * from V_PIPELITE_ASSEMBLY_STAGE where PIPELINE_NAME = 'ASSEMBLY_PROCESS' )
            select PROCESS_ID, count(1) stage_cnt, max( exec_cnt ) max_exec_cnt
             from T1
            where ENABLED = 'Y'
              and ( ( EXEC_DATE is null ) or ( ( EXEC_DATE is not null ) and EXEC_RESULT in ( 'SYSTEM_ERROR', 'UNKNOWN_ERROR' ) ) )
              and PROCESS_ID not in ( select distinct PROCESS_ID from T1 where ENABLED = 'Y' and EXEC_DATE is not null and EXEC_RESULT in ( 'USER_ERROR', 'CANCELLED' ) )
              and PROCESS_ID in ( select PROCESS_ID from T1 group by PROCESS_ID  having ( max( EXEC_CNT ) <= 3 ) )
            group by PROCESS_ID
            order by PROCESS_ID;
    */

    // TODO: does not handle null statuses
    return String.format(
        "with T1 as ( select * from %1$s where %2$s = '%3$s' )"
            + "select %4$s, count(1) stage_cnt, max( %5$s ) max_exec_cnt"
            + "  from T1 "
            + " where %6$s = 'Y' "
            + "   and ( ( %7$s is null ) or ( ( %7$s is not null ) and %8$s in ( %9$s ) ) ) "
            + "   and %4$s not in ( select distinct %4$s from T1 where %6$s = 'Y' and %7$s is not null and %8$s in ( %10$s ) ) "
            + "   and %4$s in ( select %4$s from T1 group by %4$s having ( max( %5$s ) <= %11$s ) ) "
            + " group by %4$s "
            + " order by %4$s ",

        // with
        /* 1*/ getTableName(),
        /* 2*/ PIPELINE_COLUMN_NAME,
        /* 3*/ getPipelineName(),
        // select
        /* 4*/ PROCESS_COLUMN_NAME,
        /* 5*/ ATTEMPT_COLUMN_NAME,
        /* 6*/ ENABLED_COLUMN_NAME,
        /* 7*/ EXEC_DATE_COLUMN_NAME,
        /* 8*/ EXEC_RESULT_COLUMN_NAME,
        /* 9*/ redo,
        /*10*/ terminal,
        /*11*/ getRedoCount());
  }

  public void init() throws SQLException {
    String sql = prepareQuery();
    log.info(sql);
    this.selectPS = connection.prepareStatement(sql);
  }

  public void done() {
    DbUtils.closeQuietly(selectPS);
  }

  @Override
  public List<String> getTaskQueue() throws SQLException {
    List<String> result = new ArrayList<String>();
    selectPS.execute();

    try (ResultSet rs = selectPS.getResultSet()) {
      while (rs.next()) result.add(rs.getString(1));
    }
    return result;
  }

  public String getTableName() {
    return table_name;
  }

  public void setTableName(String table_name) {
    this.table_name = table_name;
  }

  public String getPipelineName() {
    return pipeline_name;
  }

  public void setPipelineName(String pipeline_name) {
    this.pipeline_name = pipeline_name;
  }

  public int getRedoCount() {
    return redo_count;
  }

  public void setRedoCount(int redo_count) {
    this.redo_count = redo_count;
  }

  public void setCommitStatus(ExecutionResult commit_status[]) {
    this.commit_status = commit_status;
  }

  public ExecutionResult[] getCommitStatus() {
    return this.commit_status;
  }

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }
}
