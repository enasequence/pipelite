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
import pipelite.UniqueStringGenerator;
import pipelite.configuration.TaskConfiguration;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.instance.TaskInstance;
import pipelite.task.result.TaskExecutionResult;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static uk.ac.ebi.ena.sra.pipeline.launcher.LSFTaskExecutor.LSF_JVM_MEMORY_DELTA_MB;

public class LSFTaskExecutorTest {

  private TaskConfigurationEx taskConfiguration() {
    try {
      TaskConfiguration taskConfiguration =
          TaskConfiguration.builder()
              .tempDir(Files.createTempDirectory("TEMP").toString())
              .cores(1)
              .memory(1)
              .memoryTimeout(1)
              .queue("defaultQueue")
              .resolver(DefaultExceptionResolver.NAME)
              .build();
      return new TaskConfigurationEx(taskConfiguration);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private TaskInstance taskInstance(TaskConfigurationEx taskConfiguration) {
    return TaskInstance.builder()
        .processName(UniqueStringGenerator.randomProcessName())
        .processId(UniqueStringGenerator.randomProcessId())
        .taskParameters(taskConfiguration)
        .build();
  }

  private static String getCommandline(TaskExecutionResult result) {
    return result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND);
  }

  @Test
  public void test() {
    TaskConfigurationEx taskConfiguration = taskConfiguration();

    LSFTaskExecutor se = new LSFTaskExecutor(taskConfiguration);
    String cmd = getCommandline(se.execute(taskInstance(taskConfiguration)));
    assertTrue(cmd.contains(" -M 1 -R rusage[mem=1:duration=1]"));
    assertTrue(cmd.contains(" -n 1"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + taskConfiguration.getTempDir()));
    assertTrue(cmd.contains(" -eo " + taskConfiguration.getTempDir()));
  }

  @Test
  public void testNoTmpDir() {
    TaskConfigurationEx taskConfiguration = taskConfiguration();
    taskConfiguration.setTempDir(null);

    LSFTaskExecutor se = new LSFTaskExecutor(taskConfiguration);
    String cmd = getCommandline(se.execute(taskInstance(taskConfiguration)));
    // Default temporary directory is used.
    assertTrue(cmd.contains(" -oo "));
    assertTrue(cmd.contains(" -eo "));
  }

  @Test
  public void testNoQueue() {
    TaskConfigurationEx taskConfiguration = taskConfiguration();
    taskConfiguration.setQueue(null);

    LSFTaskExecutor se = new LSFTaskExecutor(taskConfiguration);
    String cmd = getCommandline(se.execute(taskInstance(taskConfiguration)));

    assertFalse(cmd.contains("-q "));
  }

  @Test
  public void testQueue() {
    TaskConfigurationEx taskConfiguration = taskConfiguration();
    taskConfiguration.setQueue("queue");

    LSFTaskExecutor se = new LSFTaskExecutor(taskConfiguration);

    String cmd = getCommandline(se.execute(taskInstance(taskConfiguration)));
    assertTrue(cmd.contains("-q queue"));
  }

  @Test
  public void testTaskSpecificMemoryAndCores() {
    TaskConfigurationEx taskConfiguration = taskConfiguration();
    taskConfiguration.setMemory(2000);
    taskConfiguration.setCores(12);

    LSFTaskExecutor se = new LSFTaskExecutor(taskConfiguration);
    String cmd = getCommandline(se.execute(taskInstance(taskConfiguration)));
    assertTrue(cmd.contains(" -M 2000 -R rusage[mem=2000:duration=1]"));
    assertTrue(cmd.contains(" -n 12"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + taskConfiguration.getTempDir()));
    assertTrue(cmd.contains(" -eo " + taskConfiguration.getTempDir()));
  }

  @Test
  public void testTaskWithJavaXmxMemory() {
    TaskConfigurationEx taskConfiguration = taskConfiguration();
    taskConfiguration.setMemory(2000);

    LSFTaskExecutor se = new LSFTaskExecutor(taskConfiguration);
    String cmd = getCommandline(se.execute(taskInstance(taskConfiguration)));
    assertTrue(cmd.contains(" -M 2000 -R rusage[mem=2000:duration=1]"));
    assertTrue(cmd.contains(" -Xmx" + 2000 + "M"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {
    TaskConfigurationEx taskConfiguration = taskConfiguration();
    taskConfiguration.setEnv(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"});

    try {
      System.setProperty("PIPELITE_TEST_JAVA_PROPERTY", "VALUE");

      LSFTaskExecutor se = new LSFTaskExecutor(taskConfiguration);
      String cmd = getCommandline(se.execute(taskInstance(taskConfiguration)));
      assertTrue(cmd.contains(" -DPIPELITE_TEST_JAVA_PROPERTY=VALUE"));
    } finally {
      System.clearProperty("PIPELITE_TEST_JAVA_PROPERTY");
    }
  }
}
