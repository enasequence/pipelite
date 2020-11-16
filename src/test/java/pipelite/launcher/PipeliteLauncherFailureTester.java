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

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.TestInMemoryProcessFactory;
import pipelite.TestInMemoryProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.executor.ErrorStageExecutor;
import pipelite.executor.SuccessStageExecutor;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.process.builder.ProcessBuilder;

@Component
@Scope("prototype")
public class PipeliteLauncherFailureTester {

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;
  @Autowired private ApplicationContext context;

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessFactory firstStageFailsProcessFactory() {
      return new TestInMemoryProcessFactory(FIRST_STAGE_FAILS_NAME, FIRST_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessFactory secondStageFailsProcessFactory() {
      return new TestInMemoryProcessFactory(SECOND_STAGE_FAILS_NAME, SECOND_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessFactory thirdStageFailsProcessFactory() {
      return new TestInMemoryProcessFactory(THIRD_STAGE_FAILS_NAME, THIRD_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessFactory fourthStageFailsProcessFactory() {
      return new TestInMemoryProcessFactory(FOURTH_STAGE_FAILS_NAME, FOURTH_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessFactory noStageFailsProcessFactory() {
      return new TestInMemoryProcessFactory(NO_STAGE_FAILS_NAME, NO_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessSource firstStageFailsProcessSource() {
      return new TestInMemoryProcessSource(FIRST_STAGE_FAILS_NAME, FIRST_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessSource secondStageFailsProcessSource() {
      return new TestInMemoryProcessSource(SECOND_STAGE_FAILS_NAME, SECOND_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessSource thirdStageFailsProcessSource() {
      return new TestInMemoryProcessSource(THIRD_STAGE_FAILS_NAME, THIRD_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessSource fourthStageFailsProcessSource() {
      return new TestInMemoryProcessSource(FOURTH_STAGE_FAILS_NAME, FOURTH_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessSource noStageFailsProcessSource() {
      return new TestInMemoryProcessSource(NO_STAGE_FAILS_NAME, NO_STAGE_FAILS_PROCESSES);
    }
  }

  private static final String FIRST_STAGE_FAILS_NAME = UniqueStringGenerator.randomPipelineName();
  private static final String SECOND_STAGE_FAILS_NAME = UniqueStringGenerator.randomPipelineName();
  private static final String THIRD_STAGE_FAILS_NAME = UniqueStringGenerator.randomPipelineName();
  private static final String FOURTH_STAGE_FAILS_NAME = UniqueStringGenerator.randomPipelineName();
  private static final String NO_STAGE_FAILS_NAME = UniqueStringGenerator.randomPipelineName();

  private static final int PROCESS_CNT = 5;

  private static final List<Process> FIRST_STAGE_FAILS_PROCESSES =
      list(PipeliteLauncherFailureTester::firstStageFailsProcessGenerator);
  private static final List<Process> SECOND_STAGE_FAILS_PROCESSES =
      list(PipeliteLauncherFailureTester::secondStageFailsProcessGenerator);
  private static final List<Process> THIRD_STAGE_FAILS_PROCESSES =
      list(PipeliteLauncherFailureTester::thirdStageFailsProcessGenerator);
  private static final List<Process> FOURTH_STAGE_FAILS_PROCESSES =
      list(PipeliteLauncherFailureTester::fourthStageFailsProcessGenerator);
  private static final List<Process> NO_STAGE_FAILS_PROCESSES =
      list(PipeliteLauncherFailureTester::noStageFailsProcessGenerator);

  private static List<Process> list(Supplier<Process> supplier) {
    return Stream.generate(() -> supplier.get()).limit(PROCESS_CNT).collect(Collectors.toList());
  }

  private static Process firstStageFailsProcessGenerator() {
    return new ProcessBuilder(FIRST_STAGE_FAILS_NAME, UniqueStringGenerator.randomProcessId(), 9)
        .execute("STAGE1")
        .with(new ErrorStageExecutor())
        .executeAfterPrevious("STAGE2")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE3")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE4")
        .with(new SuccessStageExecutor())
        .build();
  }

  private static Process secondStageFailsProcessGenerator() {
    return new ProcessBuilder(SECOND_STAGE_FAILS_NAME, UniqueStringGenerator.randomProcessId(), 9)
        .execute("STAGE1")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE2")
        .with(new ErrorStageExecutor())
        .executeAfterPrevious("STAGE3")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE4")
        .with(new SuccessStageExecutor())
        .build();
  }

  private static Process thirdStageFailsProcessGenerator() {
    return new ProcessBuilder(THIRD_STAGE_FAILS_NAME, UniqueStringGenerator.randomProcessId(), 9)
        .execute("STAGE1")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE2")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE3")
        .with(new ErrorStageExecutor())
        .executeAfterPrevious("STAGE4")
        .with(new SuccessStageExecutor())
        .build();
  }

  private static Process fourthStageFailsProcessGenerator() {
    return new ProcessBuilder(FOURTH_STAGE_FAILS_NAME, UniqueStringGenerator.randomProcessId(), 9)
        .execute("STAGE1")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE2")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE3")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE4")
        .with(new ErrorStageExecutor())
        .build();
  }

  private static Process noStageFailsProcessGenerator() {
    return new ProcessBuilder(NO_STAGE_FAILS_NAME, UniqueStringGenerator.randomProcessId(), 9)
        .execute("STAGE1")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE2")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE3")
        .with(new SuccessStageExecutor())
        .executeAfterPrevious("STAGE4")
        .with(new SuccessStageExecutor())
        .build();
  }

  private PipeliteLauncher pipeliteLauncher(String pipelineName) {
    launcherConfiguration.setPipelineName(pipelineName);
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    return pipeliteLauncher;
  }

  public void testFirstStageFails() {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncher(FIRST_STAGE_FAILS_NAME);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    TestInMemoryProcessSource processSource =
        (TestInMemoryProcessSource) context.getBean("firstStageFailsProcessSource");

    assertThat(processSource.getNewProcesses()).isEqualTo(0);
    assertThat(processSource.getReturnedProcesses()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcesses()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
  }

  public void testSecondStageFails() {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncher(SECOND_STAGE_FAILS_NAME);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    TestInMemoryProcessSource processSource =
        (TestInMemoryProcessSource) context.getBean("secondStageFailsProcessSource");

    assertThat(processSource.getNewProcesses()).isEqualTo(0);
    assertThat(processSource.getReturnedProcesses()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcesses()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
  }

  public void testThirdStageFails() {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncher(THIRD_STAGE_FAILS_NAME);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    TestInMemoryProcessSource processSource =
        (TestInMemoryProcessSource) context.getBean("thirdStageFailsProcessSource");

    assertThat(processSource.getNewProcesses()).isEqualTo(0);
    assertThat(processSource.getReturnedProcesses()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcesses()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT * 2);
    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
  }

  public void testFourthStageFails() {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncher(FOURTH_STAGE_FAILS_NAME);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    TestInMemoryProcessSource processSource =
        (TestInMemoryProcessSource) context.getBean("fourthStageFailsProcessSource");

    assertThat(processSource.getNewProcesses()).isEqualTo(0);
    assertThat(processSource.getReturnedProcesses()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcesses()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT * 3);
    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
  }

  public void testNoStageFails() {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncher(NO_STAGE_FAILS_NAME);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    TestInMemoryProcessSource processSource =
        (TestInMemoryProcessSource) context.getBean("noStageFailsProcessSource");

    assertThat(processSource.getNewProcesses()).isEqualTo(0);
    assertThat(processSource.getReturnedProcesses()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcesses()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT * 4);
    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(0);
  }
}
