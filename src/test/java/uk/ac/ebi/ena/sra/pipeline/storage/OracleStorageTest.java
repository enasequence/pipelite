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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pipelite.task.result.resolver.TaskExecutionResultResolver;
import uk.ac.ebi.ena.sra.pipeline.configuration.OracleHeartBeatConnection;
import pipelite.process.instance.ProcessInstance;
import pipelite.process.state.ProcessExecutionState;
import pipelite.task.instance.TaskInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;

public class OracleStorageTest {
  static StorageBackend db_backend;
  static Logger log = Logger.getLogger(OracleStorageTest.class);
  static final String PIPELINE_NAME = "RUN_PROCESS";
  static Connection connection;

  public static Connection createConnection() throws SQLException, ClassNotFoundException {
    return createConnection(
        "era",
        "eradevt1",
        "jdbc:oracle:thin:@ (DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = ora-dlvm5-008.ebi.ac.uk)(PORT = 1521))) (CONNECT_DATA = (SERVICE_NAME = VERADEVT) (SERVER = DEDICATED)))");
  }

  public static Connection createConnection(String user, String passwd, String url)
      throws SQLException, ClassNotFoundException {

    Properties props = new Properties();
    props.put("user", user);
    props.put("password", passwd);
    props.put("SetBigStringTryClob", "true");

    Class.forName("oracle.jdbc.driver.OracleDriver");
    Connection connection = new OracleHeartBeatConnection(DriverManager.getConnection(url, props));
    connection.setAutoCommit(false);

    return connection;
  }

  @BeforeAll
  public static void setup() throws ClassNotFoundException, SQLException {
    PropertyConfigurator.configure("resource/test.log4j.properties");

    connection = createConnection();

    OracleProcessIdSource ps = new OracleProcessIdSource();
    ps.setTableName("PIPELITE_PROCESS");
    ps.setExecutionResultArray(
        TaskExecutionResultResolver.DEFAULT_EXCEPTION_RESOLVER.resultsArray());
    ps.setRedoCount(Integer.MAX_VALUE);
    ps.setConnection(connection);
    ps.setPipelineName(PIPELINE_NAME);
    ps.init();

    OracleStorage os = new OracleStorage();
    os.setPipelineName(PIPELINE_NAME);
    os.setProcessTableName("PIPELITE_PROCESS");
    os.setStageTableName("PIPELITE_STAGE");
    os.setConnection(connection);
    db_backend = os;
  }

  @AfterAll
  public static void whack() {
    // DbUtils.commitAndCloseQuietly( connection );
    DbUtils.rollbackAndCloseQuietly(connection);
  }

  @Test
  public void test() {
    List<String> ids = Stream.of("PROCESS_ID1", "PROCESS_ID2").collect(Collectors.toList());
    AtomicInteger cnt1 = new AtomicInteger(ids.size());

    ids.forEach(
        i -> {
          try {
            loadTasks(PIPELINE_NAME, Arrays.asList(i));
          } catch (RuntimeException e) {
            cnt1.decrementAndGet();
          }
        });
    assertEquals(0, cnt1.get());

    List<ProcessInstance> saved = saveTasks(PIPELINE_NAME, ids);
    List<ProcessInstance> loaded = loadTasks(PIPELINE_NAME, ids);
    assertArrayEquals(
        saved.toArray(new ProcessInstance[saved.size()]),
        loaded.toArray(new ProcessInstance[loaded.size()]));

    Stage[] stages =
        new Stage[] {mock(Stage.class), mock(Stage.class), mock(Stage.class), mock(Stage.class)};

    // Try to load not existing stages;
    AtomicInteger cnt = new AtomicInteger(stages.length);
    Stream.of(stages)
        .forEach(
            s -> {
              try {
                loadStages(PIPELINE_NAME, ids.get(0), s);
              } catch (RuntimeException e) {
                cnt.decrementAndGet();
              }
            });
    assertEquals(0, cnt.get());

    List<TaskInstance> si = saveStages(PIPELINE_NAME, ids.get(0), stages);
    List<TaskInstance> li = loadStages(PIPELINE_NAME, ids.get(0), stages);
    assertArrayEquals(si.toArray(), li.toArray());

    List<TaskInstance> ui =
        li.stream()
            .map(
                e -> {
                  TaskInstance r = new TaskInstance(e);
                  r.setExecutionCount(r.getExecutionCount() + 1);
                  return r;
                })
            .collect(Collectors.toList());
    assertNotEquals(li, ui);

    List<TaskInstance> sui = saveStages(ui);
    List<TaskInstance> lui = loadStages(PIPELINE_NAME, ids.get(0), stages);
    assertEquals(sui, lui);
  }

  private List<TaskInstance> saveStages(List<TaskInstance> stages) {
    stages.forEach(
        s -> {
          try {
            db_backend.save(s);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    return stages;
  }

  private List<TaskInstance> saveStages(String pipeline_name, String process_id, Stage... stages) {
    List<TaskInstance> result = new ArrayList<>();

    Stream.of(stages)
        .forEach(
            s -> {
              TaskInstance si = new TaskInstance();
              si.setProcessName(pipeline_name);
              si.setEnabled(true);
              si.setProcessId(process_id);
              si.setTaskName(s.toString());
              result.add(si);
              try {
                db_backend.save(si);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    return result;
  }

  private List<TaskInstance> loadStages(String pipeline_name, String process_id, Stage... stages) {
    List<TaskInstance> result = new ArrayList<>();

    Stream.of(stages)
        .forEach(
            s -> {
              TaskInstance si = new TaskInstance();
              si.setProcessName(pipeline_name);
              si.setEnabled(true);
              si.setProcessId(process_id);
              si.setTaskName(s.toString());
              result.add(si);

              try {
                db_backend.load(si);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    return result;
  }

  private List<ProcessInstance> loadTasks(String pipeline_name, List<String> ids) {
    return ids.stream()
        .map(
            id -> {
              ProcessInstance result;
              try {
                result = new ProcessInstance();
                result.setPipelineName(pipeline_name);
                result.setProcessId(id);
                db_backend.load(result);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return result;
            })
        .collect(Collectors.toList());
  }

  private List<ProcessInstance> saveTasks(String pipeline_name, List<String> ids) {
    return ids.stream()
        .map(
            id -> {
              ProcessInstance result;
              try {
                result = new ProcessInstance();
                result.setPipelineName(pipeline_name);
                result.setProcessId(id);
                result.setProcessComment("PROCESS_COMMENT");
                result.setState(ProcessExecutionState.ACTIVE);
                db_backend.save(result);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return result;
            })
        .collect(Collectors.toList());
  }
}
