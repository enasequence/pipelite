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
import pipelite.configuration.TaskConfiguration;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.instance.TaskInstance;
import pipelite.task.TaskExecutionResult;

public class DetachedTaskExecutorTest {

  private TaskConfigurationEx taskConfiguration() {
    TaskConfiguration taskConfiguration =
        TaskConfiguration.builder().resolver(DefaultExceptionResolver.NAME).build();
    return new TaskConfigurationEx(taskConfiguration);
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
  public void javaMemory() {
    TaskConfigurationEx taskConfiguration = taskConfiguration();

    taskConfiguration.setMemory(2000);

    DetachedTaskExecutor se = new DetachedTaskExecutor();
    String cmd = getCommandline(se.execute(taskInstance(taskConfiguration)));
    assertTrue(cmd.contains(" -Xmx2000M"));
  }

  @Test
  public void javaMemoryNotSet() {
    TaskConfigurationEx taskConfiguration = taskConfiguration();

    DetachedTaskExecutor se = new DetachedTaskExecutor();
    String cmd = getCommandline(se.execute(taskInstance(taskConfiguration)));
    assertFalse(cmd.contains(" -Xmx2000M"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {
    TaskConfigurationEx taskConfiguration = taskConfiguration();

    taskConfiguration.setEnv(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"});

    try {
      System.setProperty("PIPELITE_TEST_JAVA_PROPERTY", "VALUE");

      DetachedTaskExecutor se = new DetachedTaskExecutor();
      String cmd = getCommandline(se.execute(taskInstance(taskConfiguration)));
      assertTrue(cmd.contains(" -DPIPELITE_TEST_JAVA_PROPERTY=VALUE"));
    } finally {
      System.clearProperty("PIPELITE_TEST_JAVA_PROPERTY");
    }
  }
}
