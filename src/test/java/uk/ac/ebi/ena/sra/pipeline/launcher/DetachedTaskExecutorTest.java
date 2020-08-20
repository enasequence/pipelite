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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.stage.Stage;
import pipelite.instance.TaskInstance;

public class DetachedTaskExecutorTest {

  private ProcessConfiguration defaultProcessConfiguration() {
    return ProcessConfiguration.builder().resolver(DefaultExceptionResolver.NAME).build();
  }

  private TaskConfiguration defaultTaskConfiguration() {
    return TaskConfiguration.builder().build();
  }

  private TaskInstance defaultTaskInstance(TaskConfiguration taskConfiguration) {
    Stage stage = mock(Stage.class);
    doReturn(taskConfiguration).when(stage).getTaskConfiguration();
    TaskInstance taskInstance =
        new TaskInstance(
            mock(PipeliteProcess.class),
            mock(PipeliteStage.class),
            taskConfiguration,
            stage);
    return taskInstance;
  }

  @Test
  public void javaMemory() {
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();
    TaskInstance taskInstance = defaultTaskInstance(taskConfiguration);

    taskConfiguration.setMemory(2000);

    DetachedTaskExecutor se = new DetachedTaskExecutor(processConfiguration, taskConfiguration);
    se.execute(taskInstance);

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -Xmx2000M"));
  }

  @Test
  public void javaMemoryNotSet() {
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();
    TaskInstance taskInstance = defaultTaskInstance(taskConfiguration);

    DetachedTaskExecutor se = new DetachedTaskExecutor(processConfiguration, taskConfiguration);
    se.execute(taskInstance);

    String cmdl = se.get_info().getCommandline();
    assertFalse(cmdl.contains(" -Xmx2000M"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();
    TaskInstance taskInstance = defaultTaskInstance(taskConfiguration);

    taskConfiguration.setEnv(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"});

    try {
      System.setProperty("PIPELITE_TEST_JAVA_PROPERTY", "VALUE");

      DetachedTaskExecutor se = new DetachedTaskExecutor(processConfiguration, taskConfiguration);
      se.execute(taskInstance);

      String cmd = se.get_info().getCommandline();
      assertTrue(cmd.contains(" -DPIPELITE_TEST_JAVA_PROPERTY=VALUE"));
    } finally {
      System.clearProperty("PIPELITE_TEST_JAVA_PROPERTY");
    }
  }
}
