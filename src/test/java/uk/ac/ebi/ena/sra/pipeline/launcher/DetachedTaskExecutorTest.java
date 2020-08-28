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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.executor.SuccessTaskExecutor;
import pipelite.instance.TaskParameters;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.instance.TaskInstance;
import pipelite.task.TaskExecutionResult;

public class DetachedTaskExecutorTest {

  private TaskParameters taskParameters() {
    return TaskParameters.builder().build();
  }

  private TaskInstance taskInstance(TaskParameters taskParameters) {
    return TaskInstance.builder()
        .processName(UniqueStringGenerator.randomProcessName())
        .processId(UniqueStringGenerator.randomProcessId())
        .executor(new SuccessTaskExecutor())
        .resolver(new DefaultExceptionResolver())
        .taskParameters(taskParameters)
        .build();
  }

  private static String getCommandline(TaskExecutionResult result) {
    return result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND);
  }

  @Test
  public void javaMemory() {
    TaskParameters taskParameters = taskParameters();

    taskParameters.setMemory(2000);

    DetachedTaskExecutor se = new DetachedTaskExecutor();
    String cmd = getCommandline(se.execute(taskInstance(taskParameters)));
    assertTrue(cmd.contains(" -Xmx2000M"));
  }

  @Test
  public void javaMemoryNotSet() {
    TaskParameters taskParameters = taskParameters();

    DetachedTaskExecutor se = new DetachedTaskExecutor();
    String cmd = getCommandline(se.execute(taskInstance(taskParameters)));
    assertFalse(cmd.contains(" -Xmx2000M"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {
    TaskParameters taskParameters = taskParameters();

    taskParameters.setEnv(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"});

    try {
      System.setProperty("PIPELITE_TEST_JAVA_PROPERTY", "VALUE");

      DetachedTaskExecutor se = new DetachedTaskExecutor();
      String cmd = getCommandline(se.execute(taskInstance(taskParameters)));
      assertTrue(cmd.contains(" -DPIPELITE_TEST_JAVA_PROPERTY=VALUE"));
    } finally {
      System.clearProperty("PIPELITE_TEST_JAVA_PROPERTY");
    }
  }
}
