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
package uk.ac.ebi.ena.sra.pipeline.configuration;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.apache.log4j.Logger;

/**
 * A database connection (session) with selects executed on regular intervals to help to keep long
 * running sessions alive.
 *
 * @see java.sql.Connection;
 */
public class HeartBeatConnection implements Connection {
  private final Connection connection;
  private final int heartbeat_interval;
  private static final int DEFAULT_HEARTBEAT_INTERVAL = 5 * 60 * 1000; // 5 minutes
  private final String select_query;
  private final Thread ripper;
  private volatile boolean ripped;
  private Logger log = Logger.getLogger(this.getClass());

  /**
   * @param connection The database connection.
   * @param select_query The select query executed on regular intervals.
   */
  public HeartBeatConnection(Connection connection, String select_query) {
    this(connection, DEFAULT_HEARTBEAT_INTERVAL, select_query);
  }

  /**
   * @param connection The database connection.
   * @param heartbeat_interval The interval to execute regular selects.
   * @param select_query The select query executed on regular intervals.
   */
  public HeartBeatConnection(Connection connection, int heartbeat_interval, String select_query) {
    this.connection = connection;
    this.heartbeat_interval = heartbeat_interval;
    this.select_query = select_query;
    this.ripper =
        new Thread(
            new Runnable() {
              public void run() {
                leave:
                do {
                  Statement stmt = null;
                  try {
                    stmt = HeartBeatConnection.this.connection.createStatement();
                    stmt.execute(HeartBeatConnection.this.select_query);
                    Thread.sleep(HeartBeatConnection.this.heartbeat_interval);

                  } catch (InterruptedException ie) {
                    Thread.interrupted();

                  } catch (SQLException e) {
                    e.printStackTrace();
                    break leave;
                  } finally {
                    if (null != stmt)
                      try {
                        stmt.close();
                      } catch (SQLException e) {
                        e.printStackTrace();
                        break leave;
                      }
                  }
                } while (!HeartBeatConnection.this.ripped);
              }
            });
    this.ripper.setDaemon(true);
    this.ripper.start();
  }

  @Override
  public void clearWarnings() throws SQLException {
    connection.clearWarnings();
  }

  @Override
  public void close() throws SQLException {
    //    	log.info( "close() method called from:", new Throwable().fillInStackTrace() );
    try {
      ripped = true;
      ripper.interrupt();
      ripper.join();
    } catch (InterruptedException e) {;
    } finally {
      connection.close();
    }
  }

  @Override
  public void commit() throws SQLException {
    connection.commit();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return connection.createArrayOf(typeName, elements);
  }

  @Override
  public Blob createBlob() throws SQLException {
    return connection.createBlob();
  }

  @Override
  public Clob createClob() throws SQLException {
    return connection.createClob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    return connection.createNClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return connection.createSQLXML();
  }

  @Override
  public Statement createStatement() throws SQLException {
    return connection.createStatement();
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return connection.createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return connection.createStruct(typeName, attributes);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return connection.getAutoCommit();
  }

  @Override
  public String getCatalog() throws SQLException {
    return connection.getCatalog();
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return connection.getClientInfo();
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return connection.getClientInfo(name);
  }

  @Override
  public int getHoldability() throws SQLException {
    return connection.getHoldability();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return connection.getMetaData();
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return connection.getTransactionIsolation();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return connection.getTypeMap();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return connection.getWarnings();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return connection.isClosed();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return connection.isReadOnly();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return connection.isValid(timeout);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return connection.isWrapperFor(iface);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return connection.nativeSQL(sql);
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return connection.prepareCall(sql);
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return connection.prepareStatement(
        sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return connection.prepareStatement(sql, autoGeneratedKeys);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return connection.prepareStatement(sql, columnIndexes);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return connection.prepareStatement(sql, columnNames);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return connection.prepareStatement(sql);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    connection.releaseSavepoint(savepoint);
  }

  @Override
  public void rollback() throws SQLException {
    connection.rollback();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    connection.rollback(savepoint);
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    connection.setAutoCommit(autoCommit);
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    connection.setCatalog(catalog);
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    connection.setClientInfo(properties);
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    connection.setClientInfo(name, value);
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    connection.setHoldability(holdability);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    connection.setReadOnly(readOnly);
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return connection.setSavepoint();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return connection.setSavepoint(name);
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    connection.setTransactionIsolation(level);
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    connection.setTypeMap(map);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return connection.unwrap(iface);
  }

  /* 1.8 methods */
  @Override
  public void setSchema(String schema) throws SQLException {
    connection.setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    return connection.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    connection.abort(executor);
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    connection.setNetworkTimeout(executor, milliseconds);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return connection.getNetworkTimeout();
  }
}