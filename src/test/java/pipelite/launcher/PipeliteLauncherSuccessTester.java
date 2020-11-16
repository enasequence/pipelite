/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.launcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.TestInMemoryProcessFactory;
import pipelite.TestInMemoryProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfiguration;
import pipelite.executor.StageExecutor;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.StageExecutionResult;

@Component
@Scope("prototype")
public class PipeliteLauncherSuccessTester {

  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;
  @Autowired private ProcessConfiguration processConfiguration;

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessFactory testProcessFactory() {
      return new TestInMemoryProcessFactory(PIPELINE_NAME, PROCESSES);
    }

    @Bean
    public ProcessSource testProcessSource() {
      return new TestInMemoryProcessSource(PIPELINE_NAME, PROCESSES);
    }
  }

  private static final String PIPELINE_NAME = UniqueStringGenerator.randomPipelineName();
  private static final int PROCESS_COUNT = 1;
  private static final List<Process> PROCESSES = new ArrayList<>();

  static {
    for (int i = 0; i < PROCESS_COUNT; ++i) {
      String processId = "Process" + i;
      String stageName = UniqueStringGenerator.randomStageName();
      PROCESSES.add(
          new ProcessBuilder(PIPELINE_NAME, processId, 9)
              .execute(stageName)
              .with(createStageExecutor(processId))
              .build());
    }
  }

  private static final Duration STAGE_EXECUTION_TIME = Duration.ofMillis(10);

  private static final AtomicInteger processExecutionCount = new AtomicInteger();
  private static final Set<String> processExecutionSet = ConcurrentHashMap.newKeySet();
  private static final Set<String> processExcessExecutionSet = ConcurrentHashMap.newKeySet();

  private static StageExecutor createStageExecutor(String processId) {
    return stage -> {
      processExecutionCount.incrementAndGet();
      if (processExecutionSet.contains(processId)) {
        processExcessExecutionSet.add(processId);
      } else {
        processExecutionSet.add(processId);
      }
      try {
        Thread.sleep(STAGE_EXECUTION_TIME.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return StageExecutionResult.success();
    };
  }

  public void test() {
    processExecutionCount.set(0);
    processExecutionSet.clear();
    processExcessExecutionSet.clear();

    processConfiguration.setPipelineName(PIPELINE_NAME);
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();

    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processExcessExecutionSet).isEmpty();
    assertThat(processExecutionCount.get()).isEqualTo(PROCESS_COUNT);
    assertThat(processExecutionCount.get()).isEqualTo(pipeliteLauncher.getProcessCompletedCount());
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
  }
}
