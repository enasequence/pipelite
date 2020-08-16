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
import java.sql.DriverManager;
import java.sql.SQLException;
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
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.TaskIdSource;
import pipelite.process.instance.ProcessInstance;
import pipelite.process.state.ProcessExecutionState;
import pipelite.task.instance.TaskInstance;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IdSourceTest {
  static TaskIdSource id_src;
  static StorageBackend db_backend;
  static final Logger log = Logger.getLogger(IdSourceTest.class);
  static final String PIPELINE_NAME = "TEST_PIPELINE";
  static Connection connection;

  public static Connection createConnection()
      throws SQLException, ClassNotFoundException {
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
  public static void setup()
      throws ClassNotFoundException, SQLException {
    PropertyConfigurator.configure("resource/test.log4j.properties");

    connection = createConnection();

    OracleProcessIdSource ps = new OracleProcessIdSource();
    ps.setTableName("PIPELITE_PROCESS");
    ps.setExecutionResultArray(TaskExecutionResultResolver.DEFAULT_EXCEPTION_RESOLVER.resultsArray());
    ps.setRedoCount(Integer.MAX_VALUE);
    ps.setConnection(connection);
    ps.setPipelineName(PIPELINE_NAME);

    ps.init();
    id_src = ps;

    OracleStorage os = new OracleStorage();
    os.setPipelineName(PIPELINE_NAME);
    os.setProcessTableName("PIPELITE_PROCESS");
    os.setStageTableName("PIPELITE_STAGE");
    os.setConnection(connection);
    db_backend = os;
  }

  @AfterAll
  public static void whack() {
    DbUtils.rollbackAndCloseQuietly(connection);
  }

  @Test
  public void test()
      throws SQLException {
    List<String> ids = Stream.of("PROCESS_ID1", "PROCESS_ID2").collect(Collectors.toList());
    ids
        .forEach(
            i -> {
              TaskInstance si = new TaskInstance();
              si.setProcessName(PIPELINE_NAME);
              si.setTaskName("STAGE");
              si.setProcessId(i);
              try {
                db_backend.save(si);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    List<ProcessInstance> saved = saveTasks(PIPELINE_NAME, ids);

    AtomicInteger cnt = new AtomicInteger(ids.size());

    saved.forEach(e -> e.setPriority(0));
    saved
        .forEach(
            e -> {
              try {
                db_backend.save(e);
              } catch (Exception e1) {
                throw new RuntimeException(e1);
              }
            });

    List<String> stored = id_src.getTaskQueue();
    ids
        .forEach(
            i -> stored.stream()
                .filter(e -> e.equals(i))
                .findFirst()
                .ifPresent(e -> cnt.decrementAndGet()));

    assertEquals(0, cnt.get());

    AtomicInteger priority = new AtomicInteger();
    saved.forEach(e -> e.setPriority(priority.getAndAdd(4)));
    saved
        .forEach(
            e -> {
              try {
                db_backend.save(e);
              } catch (Exception e1) {
                throw new RuntimeException(e1);
              }
            });

    assertEquals(stored.get(stored.size() - 1), id_src.getTaskQueue().get(0));
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

                log.info(result);

              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return result;
            })
        .collect(Collectors.toList());
  }
}
