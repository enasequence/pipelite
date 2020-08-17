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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.TestConfiguration;
import pipelite.process.instance.ProcessInstance;
import pipelite.process.state.ProcessExecutionState;
import pipelite.task.instance.TaskInstance;
import pipelite.stage.Stage;

import javax.sql.DataSource;
import javax.transaction.Transactional;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class OracleStorageTest {

  @Autowired DataSource dataSource;

  static final String PIPELINE_NAME = "RUN_PROCESS";

  @Test
  @Transactional
  @Rollback
  public void test() throws SQLException {
    Connection connection = DataSourceUtils.getConnection(dataSource);

    OracleProcessIdSource ps = new OracleProcessIdSource();
    ps.setRedoCount(Integer.MAX_VALUE);
    ps.setConnection(connection);
    ps.setPipelineName(PIPELINE_NAME);
    ps.init();

    OracleStorage os = new OracleStorage();
    os.setPipelineName(PIPELINE_NAME);
    os.setConnection(connection);

    List<String> ids = Stream.of("PROCESS_ID1", "PROCESS_ID2").collect(Collectors.toList());
    AtomicInteger cnt1 = new AtomicInteger(ids.size());

    ids.forEach(
        i -> {
          try {
            loadTasks(os, PIPELINE_NAME, Arrays.asList(i));
          } catch (RuntimeException e) {
            cnt1.decrementAndGet();
          }
        });
    assertEquals(2, cnt1.get());

    List<ProcessInstance> saved = saveTasks(os, PIPELINE_NAME, ids);
    List<ProcessInstance> loaded = loadTasks(os, PIPELINE_NAME, ids);
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
                loadStages(os, PIPELINE_NAME, ids.get(0), s);
              } catch (RuntimeException e) {
                cnt.decrementAndGet();
              }
            });
    assertEquals(0, cnt.get());

    List<TaskInstance> si = saveStages(os, PIPELINE_NAME, ids.get(0), stages);
    List<TaskInstance> li = loadStages(os, PIPELINE_NAME, ids.get(0), stages);
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

    List<TaskInstance> sui = saveStages(os, ui);
    List<TaskInstance> lui = loadStages(os, PIPELINE_NAME, ids.get(0), stages);
    assertEquals(sui, lui);
  }

  private List<TaskInstance> saveStages(OracleStorage os, List<TaskInstance> stages) {
    stages.forEach(
        s -> {
          try {
            os.save(s);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    return stages;
  }

  private List<TaskInstance> saveStages(OracleStorage os, String pipeline_name, String process_id, Stage... stages) {
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
                os.save(si);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    return result;
  }

  private List<TaskInstance> loadStages(OracleStorage os, String pipeline_name, String process_id, Stage... stages) {
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
                os.load(si);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    return result;
  }

  private List<ProcessInstance> loadTasks(OracleStorage os, String pipeline_name, List<String> ids) {
    return ids.stream()
        .map(
            id -> {
              ProcessInstance result;
              try {
                result = new ProcessInstance();
                result.setPipelineName(pipeline_name);
                result.setProcessId(id);
                os.load(result);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return result;
            })
        .collect(Collectors.toList());
  }

  private List<ProcessInstance> saveTasks(OracleStorage os, String pipeline_name, List<String> ids) {
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
                os.save(result);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return result;
            })
        .collect(Collectors.toList());
  }
}
