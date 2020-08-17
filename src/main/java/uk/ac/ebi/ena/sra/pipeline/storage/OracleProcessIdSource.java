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

public class OracleProcessIdSource implements OracleCommons, TaskIdSource {
  final Logger log = Logger.getLogger(this.getClass());
  private PreparedStatement selectPS;
  private String pipeline_name;
  private int redo_count;
  private Connection connection;
  private final int window = 2000;

  private String prepareQuery() {
    return String.format(
        " select * from ( "
            + " with T0 as ( "
            + " 	select * "
            + "	  from %1$s "
            + "     where state in ( 'ACTIVE' ) "
            + "       and %2$s = '%3$s' "
            + " 	   and ( mod( %6$s, %7$s ) = 0 or audit_time < sysdate - 1/24/12 ) "
            + " ) "
            + " select %4$s "
            + "   from T0 "
            + "   where not exists ( select 1 from pipelite_lock where %2$s = '%3$s' and lock_id = %4$s ) "
            + " order by %5$s desc nulls first, "
            + "          %4$s "
            + " ) where rownum < ?",
        /* 1 */ PIPELINE_PROCESS_TABLE_NAME,
        /* 2 */ PIPELINE_COLUMN_NAME,
        /* 3 */ getPipelineName(),
        /* 4 */ PROCESS_COLUMN_NAME,
        /* 5 */ PROCESS_PRIORITY_COLUMN_NAME,
        /* 6 */ ATTEMPT_COLUMN_NAME,
        /* 7 */ getRedoCount());
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
    List<String> result = new ArrayList<>();
    selectPS.setObject(1, window);
    selectPS.execute();

    try (ResultSet rs = selectPS.getResultSet()) {
      while (rs.next()) result.add(rs.getString(1));
    }
    return result;
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

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }
}
