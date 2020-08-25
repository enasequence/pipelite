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
package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.io.IOException;
import java.nio.file.Files;

import org.junit.jupiter.api.Test;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.stage.Stage;
import pipelite.instance.TaskInstance;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static uk.ac.ebi.ena.sra.pipeline.launcher.LSFTaskExecutor.LSF_JVM_MEMORY_DELTA_MB;

public class LSFTaskExecutorTest {

  private ProcessConfiguration defaultProcessConfiguration() {
    return ProcessConfiguration.builder().resolver(DefaultExceptionResolver.NAME).build();
  }

  private TaskConfiguration defaultTaskConfiguration() {
    try {
      return TaskConfiguration.builder()
          .tempDir(Files.createTempDirectory("TEMP").toString())
          .cores(1)
          .memory(1)
          .memoryTimeout(1)
          .queue("defaultQueue")
          .build();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private TaskInstance defaultTaskInstance(TaskConfiguration taskConfiguration) {
    Stage stage = mock(Stage.class);
    doReturn(taskConfiguration).when(stage).getTaskConfiguration();
    TaskInstance taskInstance =
        new TaskInstance(
            mock(PipeliteProcess.class), mock(PipeliteStage.class), taskConfiguration, stage);
    return taskInstance;
  }

  @Test
  public void test() {
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();

    LSFTaskExecutor se = new LSFTaskExecutor(processConfiguration, taskConfiguration);
    String cmd = se.execute(defaultTaskInstance(taskConfiguration)).getCommandline();
    assertTrue(cmd.contains(" -M 1 -R rusage[mem=1:duration=1]"));
    assertTrue(cmd.contains(" -n 1"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + taskConfiguration.getTempDir()));
    assertTrue(cmd.contains(" -eo " + taskConfiguration.getTempDir()));
  }

  @Test
  public void testNoTmpDir() {
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();
    taskConfiguration.setTempDir(null);

    LSFTaskExecutor se = new LSFTaskExecutor(processConfiguration, taskConfiguration);
    String cmd = se.execute(defaultTaskInstance(taskConfiguration)).getCommandline();
    // Default temporary directory is used.
    assertTrue(cmd.contains(" -oo "));
    assertTrue(cmd.contains(" -eo "));
  }

  @Test
  public void testNoQueue() {
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();
    taskConfiguration.setQueue(null);

    LSFTaskExecutor se = new LSFTaskExecutor(processConfiguration, taskConfiguration);
    String cmd = se.execute(defaultTaskInstance(taskConfiguration)).getCommandline();

    assertFalse(cmd.contains("-q "));
  }

  @Test
  public void testQueue() {
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();
    taskConfiguration.setQueue("queue");

    LSFTaskExecutor se = new LSFTaskExecutor(processConfiguration, taskConfiguration);

    String cmd = se.execute(defaultTaskInstance(taskConfiguration)).getCommandline();
    assertTrue(cmd.contains("-q queue"));
  }

  @Test
  public void testTaskSpecificMemoryAndCores() {
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();
    taskConfiguration.setMemory(2000);
    taskConfiguration.setCores(12);

    LSFTaskExecutor se = new LSFTaskExecutor(processConfiguration, taskConfiguration);
    String cmd = se.execute(defaultTaskInstance(taskConfiguration)).getCommandline();
    assertTrue(cmd.contains(" -M 2000 -R rusage[mem=2000:duration=1]"));
    assertTrue(cmd.contains(" -n 12"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + taskConfiguration.getTempDir()));
    assertTrue(cmd.contains(" -eo " + taskConfiguration.getTempDir()));
  }

  @Test
  public void testTaskWithJavaXmxMemory() {
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();
    taskConfiguration.setMemory(2000);

    LSFTaskExecutor se = new LSFTaskExecutor(processConfiguration, taskConfiguration);
    String cmd = se.execute(defaultTaskInstance(taskConfiguration)).getCommandline();
    assertTrue(cmd.contains(" -M 2000 -R rusage[mem=2000:duration=1]"));
    assertTrue(cmd.contains(" -Xmx" + (2000 - LSF_JVM_MEMORY_DELTA_MB) + "M"));
  }

  @Test
  public void testTaskWithoutJavaXmxMemory() {
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();
    taskConfiguration.setMemory(1500);

    LSFTaskExecutor se = new LSFTaskExecutor(processConfiguration, taskConfiguration);
    String cmd = se.execute(defaultTaskInstance(taskConfiguration)).getCommandline();
    assertTrue(cmd.contains(" -M 1500 -R rusage[mem=1500:duration=1]"));
    // Not enough memory requested to create -Xmx.
    assertFalse(cmd.contains(" -Xmx"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();
    taskConfiguration.setEnv(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"});

    try {
      System.setProperty("PIPELITE_TEST_JAVA_PROPERTY", "VALUE");

      LSFTaskExecutor se = new LSFTaskExecutor(processConfiguration, taskConfiguration);
      String cmd = se.execute(defaultTaskInstance(taskConfiguration)).getCommandline();
      assertTrue(cmd.contains(" -DPIPELITE_TEST_JAVA_PROPERTY=VALUE"));
    } finally {
      System.clearProperty("PIPELITE_TEST_JAVA_PROPERTY");
    }
  }
}
