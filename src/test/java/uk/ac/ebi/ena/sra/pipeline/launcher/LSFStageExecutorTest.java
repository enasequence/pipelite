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
import pipelite.configuration.LSFTaskExecutorConfiguration;
import pipelite.configuration.TaskExecutorConfiguration;
import pipelite.task.instance.TaskInstance;
import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import pipelite.task.result.resolver.TaskExecutionResultResolver;

import static org.junit.jupiter.api.Assertions.*;
import static uk.ac.ebi.ena.sra.pipeline.launcher.LSFStageExecutor.LSF_JVM_MEMORY_DELTA_MB;

public class LSFStageExecutorTest {
  private TaskExecutionResultExceptionResolver resolver() {
    return TaskExecutionResultResolver.DEFAULT_EXCEPTION_RESOLVER;
  }

  private TaskExecutorConfiguration defaultTaskExecutorConfiguration() {
    try {
      return TaskExecutorConfiguration.builder()
          .tempDir(Files.createTempDirectory("TEMP").toString())
          .cores(1)
          .memory(1)
          .build();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private LSFTaskExecutorConfiguration defaultLSFTaskExecutorConfiguration() {
    return LSFTaskExecutorConfiguration.builder().queue("defaultQueue").memoryTimeout(1).build();
  }

  private TaskInstance makeDefaultStageInstance() {
    return new TaskInstance() {
      {
        setEnabled(true);
      }
    };
  }

  @Test
  public void testDefaultConfiguration() {
    TaskExecutorConfiguration taskExecutorConfiguration = defaultTaskExecutorConfiguration();
    LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration =
        defaultLSFTaskExecutorConfiguration();

    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), taskExecutorConfiguration, lsfTaskExecutorConfiguration);

    se.execute(makeDefaultStageInstance());

    String cmd = se.get_info().getCommandline();
    assertTrue(cmd.contains(" -M 1 -R rusage[mem=1:duration=1]"));
    assertTrue(cmd.contains(" -n 1"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + taskExecutorConfiguration.getTempDir()));
    assertTrue(cmd.contains(" -eo " + taskExecutorConfiguration.getTempDir()));
  }

  @Test
  public void testNoTmpDir() {
    TaskExecutorConfiguration taskExecutorConfiguration = defaultTaskExecutorConfiguration();
    taskExecutorConfiguration.setTempDir(null);
    LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration =
        defaultLSFTaskExecutorConfiguration();

    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), taskExecutorConfiguration, lsfTaskExecutorConfiguration);

    se.execute(makeDefaultStageInstance());

    String cmd = se.get_info().getCommandline();
    // Default temporary directory is used.
    assertTrue(cmd.contains(" -oo "));
    assertTrue(cmd.contains(" -eo "));
  }

  @Test
  public void testNoQueue() {

    TaskExecutorConfiguration taskExecutorConfiguration = defaultTaskExecutorConfiguration();
    LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration =
        defaultLSFTaskExecutorConfiguration();
    lsfTaskExecutorConfiguration.setQueue(null);

    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), taskExecutorConfiguration, lsfTaskExecutorConfiguration);

    se.execute(makeDefaultStageInstance());
    assertFalse(se.get_info().getCommandline().contains("-q "));
  }

  @Test
  public void testQueue() {
    TaskExecutorConfiguration taskExecutorConfiguration = defaultTaskExecutorConfiguration();
    LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration =
        defaultLSFTaskExecutorConfiguration();
    lsfTaskExecutorConfiguration.setQueue("queue");

    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), taskExecutorConfiguration, lsfTaskExecutorConfiguration);

    se.execute(makeDefaultStageInstance());
    assertTrue(se.get_info().getCommandline().contains("-q queue"));
  }

  @Test
  public void testTaskSpecificMemoryAndCores() {
    TaskExecutorConfiguration taskExecutorConfiguration = defaultTaskExecutorConfiguration();
    LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration =
        defaultLSFTaskExecutorConfiguration();

    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), taskExecutorConfiguration, lsfTaskExecutorConfiguration);

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setMemory(2000);
            setCores(12);
          }
        });

    String cmd = se.get_info().getCommandline();
    assertTrue(cmd.contains(" -M 2000 -R rusage[mem=2000:duration=1]"));
    assertTrue(cmd.contains(" -n 12"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + taskExecutorConfiguration.getTempDir()));
    assertTrue(cmd.contains(" -eo " + taskExecutorConfiguration.getTempDir()));
  }

  @Test
  public void testTaskWithJavaXmxMemory() {
    TaskExecutorConfiguration taskExecutorConfiguration = defaultTaskExecutorConfiguration();
    LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration =
        defaultLSFTaskExecutorConfiguration();

    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), taskExecutorConfiguration, lsfTaskExecutorConfiguration);

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setMemory(2000);
          }
        });

    String cmd = se.get_info().getCommandline();
    assertTrue(cmd.contains(" -M 2000 -R rusage[mem=2000:duration=1]"));
    assertTrue(cmd.contains(" -Xmx" + (2000 - LSF_JVM_MEMORY_DELTA_MB) + "M"));
  }

  @Test
  public void testTaskWithoutJavaXmxMemory() {
    TaskExecutorConfiguration taskExecutorConfiguration = defaultTaskExecutorConfiguration();
    LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration =
        defaultLSFTaskExecutorConfiguration();

    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), taskExecutorConfiguration, lsfTaskExecutorConfiguration);

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setMemory(1500);
          }
        });

    String cmd = se.get_info().getCommandline();
    assertTrue(cmd.contains(" -M 1500 -R rusage[mem=1500:duration=1]"));
    // Not enough memory requested to create -Xmx.
    assertFalse(cmd.contains(" -Xmx"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {
    TaskExecutorConfiguration taskExecutorConfiguration = defaultTaskExecutorConfiguration();
    LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration =
        defaultLSFTaskExecutorConfiguration();

    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), taskExecutorConfiguration, lsfTaskExecutorConfiguration);

    try {
      System.setProperty("PIPELITE_TEST_JAVA_PROPERTY", "VALUE");

      se.execute(
          new TaskInstance() {
            {
              setEnabled(true);
              setJavaSystemProperties(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"});
            }
          });

      String cmd = se.get_info().getCommandline();
      assertTrue(cmd.contains(" -DPIPELITE_TEST_JAVA_PROPERTY=VALUE"));
    } finally {
      System.clearProperty("PIPELITE_TEST_JAVA_PROPERTY");
    }
  }
}
