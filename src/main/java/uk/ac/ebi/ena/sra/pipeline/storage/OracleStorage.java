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

  private Connection connection;
  private String pipeline_name;

  public OracleStorage() {
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
