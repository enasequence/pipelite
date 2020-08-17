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
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.TestConfiguration;
import pipelite.resolver.TaskExecutionResultResolver;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.TaskIdSource;
import pipelite.process.instance.ProcessInstance;
import pipelite.process.state.ProcessExecutionState;
import pipelite.task.instance.TaskInstance;

import javax.sql.DataSource;
import javax.transaction.Transactional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class IdSourceTest {

  @Autowired DataSource dataSource;

  static TaskIdSource id_src;
  static final Logger log = Logger.getLogger(IdSourceTest.class);
  static final String PIPELINE_NAME = "TEST_PIPELINE";

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
    id_src = ps;

    OracleStorage os = new OracleStorage();
    os.setPipelineName(PIPELINE_NAME);
    os.setConnection(connection);

    List<String> ids = Stream.of("PROCESS_ID1", "PROCESS_ID2").collect(Collectors.toList());
    ids.forEach(
        i -> {
          TaskInstance si = new TaskInstance();
          si.setProcessName(PIPELINE_NAME);
          si.setTaskName("STAGE");
          si.setProcessId(i);
          try {
            os.save(si);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    List<ProcessInstance> saved = saveTasks(os, PIPELINE_NAME, ids);

    AtomicInteger cnt = new AtomicInteger(ids.size());

    saved.forEach(e -> e.setPriority(0));
    saved.forEach(
        e -> {
          try {
            os.save(e);
          } catch (Exception e1) {
            throw new RuntimeException(e1);
          }
        });

    List<String> stored = id_src.getTaskQueue();
    ids.forEach(
        i ->
            stored.stream()
                .filter(e -> e.equals(i))
                .findFirst()
                .ifPresent(e -> cnt.decrementAndGet()));

    assertEquals(0, cnt.get());

    AtomicInteger priority = new AtomicInteger();
    saved.forEach(e -> e.setPriority(priority.getAndAdd(4)));
    saved.forEach(
        e -> {
          try {
            os.save(e);
          } catch (Exception e1) {
            throw new RuntimeException(e1);
          }
        });

    assertEquals(stored.get(stored.size() - 1), id_src.getTaskQueue().get(0));
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

                log.info(result);

              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return result;
            })
        .collect(Collectors.toList());
  }
}
