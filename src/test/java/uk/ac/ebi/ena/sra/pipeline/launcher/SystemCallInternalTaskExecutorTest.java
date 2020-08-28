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
import pipelite.executor.SystemCallInternalTaskExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.instance.TaskParameters;
import pipelite.instance.TaskInstance;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;

public class SystemCallInternalTaskExecutorTest {

  private TaskParameters taskParameters() {
    return TaskParameters.builder().build();
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

  private static String getCommand(TaskExecutionResult result) {
    return result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND);
  }

  @Test
  public void javaMemory() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setMemory(2000);

    SystemCallInternalTaskExecutor executor = SystemCallInternalTaskExecutor.builder().build();
    String cmd = getCommand(executor.execute(taskInstance(taskParameters)));
    assertTrue(cmd.contains(" -Xmx2000M"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {
    TaskParameters taskParameters = taskParameters();

    taskParameters.setEnv(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"});

    try {
      System.setProperty("PIPELITE_TEST_JAVA_PROPERTY", "VALUE");

      SystemCallInternalTaskExecutor executor = SystemCallInternalTaskExecutor.builder().build();
      String cmd = getCommand(executor.execute(taskInstance(taskParameters)));
      assertTrue(cmd.contains(" -DPIPELITE_TEST_JAVA_PROPERTY=VALUE"));
    } finally {
      System.clearProperty("PIPELITE_TEST_JAVA_PROPERTY");
    }
  }
}
