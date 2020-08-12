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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;

public class ProcessLogBean {
  private static final int MESSAGE_FIELD_LENGTH = 255;
  private static final int EXCEPTION_FIELD_LENGTH = 256 * 1024;

  static final String PIPELINE_FIELD_NAME = "PIPELINE_NAME";
  static final String PROCESS_FIELD_NAME = "PROCESS_ID";
  static final String STAGE_FIELD_NAME = "STAGE_NAME";
  static final String EXEC_ID_FIELD_NAME = "EXEC_ID";
  static final String EXCEPTION_FIELD_NAME = "EXCEPTION";
  static final String LSF_JOB_ID_FIELD_NAME = "JOBID";
  static final String LSF_HOST_FIELD_NAME = "HOSTS";
  static final String DATE_FIELD_NAME = "LOG_DATE";
  static final String MESSAGE_FIELD_NAME = "MESSAGE";

  Map<String, Object> data;

  public ProcessLogBean() {
    this(new HashMap<String, Object>());
  }

  public ProcessLogBean(Map<String, Object> data) {
    this.data = data;
  }

  public String getPipelineName() throws NoSuchFieldException {
    return getField(PIPELINE_FIELD_NAME);
  }

  public void setPipelineName(String pipeline_name) {
    setField(PIPELINE_FIELD_NAME, pipeline_name);
  }

  public String getStage() throws NoSuchFieldException {
    return getField(STAGE_FIELD_NAME);
  }

  public void setStage(String stage) {
    setField(STAGE_FIELD_NAME, stage);
  }

  public String getProcessID() throws NoSuchFieldException {
    return getField(PROCESS_FIELD_NAME);
  }

  public void setProcessID(Object ID) {
    setField(PROCESS_FIELD_NAME, ID);
  }

  public void setExecutionId(String execution_id) {
    setField(EXEC_ID_FIELD_NAME, execution_id);
  }

  public String getExecutionId() throws NoSuchFieldException {
    return getField(EXEC_ID_FIELD_NAME);
  }

  public String getMessage() throws NoSuchFieldException {
    return getField(MESSAGE_FIELD_NAME);
  }

  public void setMessage(String message) {
    setField(
        MESSAGE_FIELD_NAME,
        null != message && message.length() > MESSAGE_FIELD_LENGTH
            ? message.substring(0, MESSAGE_FIELD_LENGTH)
            : message);
  }

  public String getExceptionText() throws NoSuchFieldException {
    return getField(EXCEPTION_FIELD_NAME);
  }

  public void setThrowable(Throwable t) {
    if (null != t) {
      setMessage(t.getMessage());
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      t.printStackTrace(pw);
      setExceptionText(sw.toString());
      pw.close();
    } else {
      setMessage(null);
      setExceptionText(null);
    }
  }

  public void setExceptionText(String text) {
    setField(
        EXCEPTION_FIELD_NAME,
        null != text && text.length() > EXCEPTION_FIELD_LENGTH
            ? text.substring(0, EXCEPTION_FIELD_LENGTH)
            : text);
  }

  public Long getLSFJobID() throws NoSuchFieldException {
    return getField(LSF_JOB_ID_FIELD_NAME);
  }

  public void setLSFJobID(Long value) {
    setField(LSF_JOB_ID_FIELD_NAME, value);
  }

  public String getLSFHost() throws NoSuchFieldException {
    return getField(LSF_HOST_FIELD_NAME);
  }

  public void setLSFHosts(String value) {
    setField(LSF_HOST_FIELD_NAME, value);
  }

  @SuppressWarnings("unchecked")
  public <T> T getField(String name) throws NoSuchFieldException {
    if (data.containsKey(name)) return (T) data.get(name);

    throw new NoSuchFieldException(name);
  }

  public void setField(String name, Object value) {
    data.put(name, value);
  }

  @Deprecated
  void persist(Connection connection) throws SQLException {
    PreparedStatement stmt = null;
    synchronized (data) {
      StringBuilder sb = new StringBuilder("insert into ");
      sb.append(DefaultConfiguration.currentSet().getLogTableName())
          .append("( ")
          .append(DATE_FIELD_NAME);

      for (Entry<String, Object> e : data.entrySet()) sb.append(", ").append(e.getKey());

      sb.append(" ) values ( ").append("sysdate");

      for (@SuppressWarnings("unused") Entry<String, Object> e : data.entrySet()) sb.append(", ?");

      sb.append(" )");

      try {
        stmt = connection.prepareStatement(sb.toString());

        int index = 0;

        for (Entry<String, Object> e : data.entrySet()) stmt.setObject(++index, e.getValue());

        if (1 != stmt.executeUpdate())
          throw new SQLException(
              String.format("Unable to insert row: %s, values: %s", sb.toString(), data));

      } finally {
        if (null != stmt)
          try {
            stmt.close();
          } catch (SQLException e) {
            e.printStackTrace();
          }
      }
    }
  }

  public String toString() {
    return String.format("%s", data);
  }
}
