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

public class OracleStorage implements OracleCommons, StorageBackend {

  private static final String LOCK_EXPRESSION = "for update nowait";

  @Deprecated private String log_table_name;

  private static final String ENABLED_VALUE = "Y";

  private Connection connection;
  private String pipeline_name;

  public OracleStorage() {
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
