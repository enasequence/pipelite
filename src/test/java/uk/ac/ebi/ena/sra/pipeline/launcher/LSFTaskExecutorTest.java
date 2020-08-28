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
import pipelite.executor.TaskExecutor;
import pipelite.instance.TaskParameters;
import pipelite.instance.TaskInstance;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;

import static org.junit.jupiter.api.Assertions.*;

public class LSFTaskExecutorTest {

  private TaskParameters taskParameters() {
    try {
      TaskParameters taskParameters =
          TaskParameters.builder()
              .tempDir(Files.createTempDirectory("TEMP").toString())
              .cores(1)
              .memory(1)
              .memoryTimeout(1)
              .queue("defaultQueue")
              .build();
      return taskParameters;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private TaskInstance taskInstance(TaskParameters taskParameters) {
    return TaskInstance.builder()
        .processName(UniqueStringGenerator.randomProcessName())
        .processId(UniqueStringGenerator.randomProcessId())
        .executor(TaskExecutor.DEFAULT_SUCCESS_EXECUTOR)
        .resolver(ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
        .taskParameters(taskParameters)
        .build();
  }

  private static String getCommandline(TaskExecutionResult result) {
    return result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND);
  }

  @Test
  public void test() {
    TaskParameters taskParameters = taskParameters();

    LSFTaskExecutor se = new LSFTaskExecutor();
    String cmd = getCommandline(se.execute(taskInstance(taskParameters)));
    assertTrue(cmd.contains(" -M 1 -R rusage[mem=1:duration=1]"));
    assertTrue(cmd.contains(" -n 1"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + taskParameters.getTempDir()));
    assertTrue(cmd.contains(" -eo " + taskParameters.getTempDir()));
  }

  @Test
  public void testNoTmpDir() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setTempDir(null);

    LSFTaskExecutor se = new LSFTaskExecutor();
    String cmd = getCommandline(se.execute(taskInstance(taskParameters)));
    // Default temporary directory is used.
    assertTrue(cmd.contains(" -oo "));
    assertTrue(cmd.contains(" -eo "));
  }

  @Test
  public void testNoQueue() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setQueue(null);

    LSFTaskExecutor se = new LSFTaskExecutor();
    String cmd = getCommandline(se.execute(taskInstance(taskParameters)));

    assertFalse(cmd.contains("-q "));
  }

  @Test
  public void testQueue() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setQueue("queue");

    LSFTaskExecutor se = new LSFTaskExecutor();

    String cmd = getCommandline(se.execute(taskInstance(taskParameters)));
    assertTrue(cmd.contains("-q queue"));
  }

  @Test
  public void testTaskSpecificMemoryAndCores() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setMemory(2000);
    taskParameters.setCores(12);

    LSFTaskExecutor se = new LSFTaskExecutor();
    String cmd = getCommandline(se.execute(taskInstance(taskParameters)));
    assertTrue(cmd.contains(" -M 2000 -R rusage[mem=2000:duration=1]"));
    assertTrue(cmd.contains(" -n 12"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + taskParameters.getTempDir()));
    assertTrue(cmd.contains(" -eo " + taskParameters.getTempDir()));
  }

  @Test
  public void testTaskWithJavaXmxMemory() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setMemory(2000);

    LSFTaskExecutor se = new LSFTaskExecutor();
    String cmd = getCommandline(se.execute(taskInstance(taskParameters)));
    assertTrue(cmd.contains(" -M 2000 -R rusage[mem=2000:duration=1]"));
    assertTrue(cmd.contains(" -Xmx" + 2000 + "M"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setEnv(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"});

    try {
      System.setProperty("PIPELITE_TEST_JAVA_PROPERTY", "VALUE");

      LSFTaskExecutor se = new LSFTaskExecutor();
      String cmd = getCommandline(se.execute(taskInstance(taskParameters)));
      assertTrue(cmd.contains(" -DPIPELITE_TEST_JAVA_PROPERTY=VALUE"));
    } finally {
      System.clearProperty("PIPELITE_TEST_JAVA_PROPERTY");
    }
  }
}
