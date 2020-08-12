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
package uk.ac.ebi.ena.sra.pipeline.base.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;

public class DBRowLock {
  public static class LockException extends Exception {
    private static final long serialVersionUID = 1L;

    public LockException(String message) {
      super(message);
    }

    public LockException(Throwable cause) {
      super(cause);
    }

    public LockException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private final String table_name;
  private final String lock_expression;
  private final String lock_sql_template = "select * from %s where ( %s ) for update nowait";
  private final String insert_sql_template = "insert into %s %s ";
  PreparedStatement stmt;
  ResultSet rs;
  Connection connection;
  boolean owner = false;

  public Connection getConnection() {
    return connection;
  }

  public DBRowLock(String table_name)
      throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
    this(DefaultConfiguration.currentSet().createConnection(), table_name, null);
    this.owner = true;
  }

  public DBRowLock(Connection connection, String table_name, String lock_expression) {
    this.connection = connection;
    this.table_name = table_name;
    this.lock_expression = lock_expression;
  }

  public boolean lock() throws LockException {
    return lock(lock_expression);
  }

  public boolean lock(String lock_expression, String insert_expression) throws LockException {
    try {
      return lock(lock_expression, false);
    } catch (LockException le) {
      String sql = String.format(insert_sql_template, table_name, insert_expression);
      PreparedStatement stmt = null;
      try {
        stmt = connection.prepareStatement(sql);
        if (0 >= stmt.executeUpdate()) {
          release(false);
          throw new LockException("Unable to perform insertion using " + sql);
        }
        connection.commit();
        return lock(lock_expression);

      } catch (SQLException sqle) {
        throw new LockException("Unknown exception while insertion using " + sql, sqle);
      } finally {
        if (null != stmt) {
          try {
            stmt.close();
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  public boolean lock(String lock_expression) throws LockException {
    return lock(lock_expression, true);
  }

  public boolean lock(String lock_expression, boolean close_connection) throws LockException {
    String sql = String.format(lock_sql_template, table_name, lock_expression);
    try {
      stmt = connection.prepareStatement(sql);
      rs = stmt.executeQuery();
      if (rs.next()) return true;

      release(false, close_connection);
      throw new LockException(
          String.format("No row in table %s with %s ", table_name, lock_expression));

    } catch (SQLException e) {
      release(false, close_connection);
      if (e.getErrorCode() == 54 || e.getErrorCode() == 30006) {
        return false;
      } else {
        throw new LockException(
            String.format(
                "Unknown exception while locking with query [%s] where [%s]", sql, lock_expression),
            e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T get_value(String name) throws SQLException {
    T t = (T) rs.getObject(name);
    return rs.wasNull() ? null : t;
  }

  public void release(boolean commit) throws LockException {
    release(commit, true);
  }

  private void release(boolean commit, boolean close_connection) throws LockException {
    try {
      if (commit) connection.commit();
      else connection.rollback();

    } catch (SQLException e) {
      throw new LockException(e);

    } finally {
      if (null != rs) {
        try {
          rs.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }

      if (null != stmt) {
        try {
          stmt.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }

      if (close_connection && owner && null != connection) {
        try {
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
