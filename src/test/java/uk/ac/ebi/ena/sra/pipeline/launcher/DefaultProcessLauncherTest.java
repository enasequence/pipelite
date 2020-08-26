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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import lombok.extern.flogger.Flogger;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.FullTestConfiguration;

@Flogger
@SpringBootTest(classes = FullTestConfiguration.class)
@ActiveProfiles(value = {"database", "database-oracle-test"})
public class DefaultProcessLauncherTest {

  /*
  private static final String PROCESS_NAME = "TEST_PROCESS";
  private static final String PROCESS_ID = "TEST_PROCESS_ID";

  private static final ExceptionResolver resolver =
      ConcreteExceptionResolver.builder()
          .transientError(TransientException.class, "TRANSIENT_ERROR")
          .permanentError(PermanentException.class, "PERMANENT_ERROR")
          .build();

  private LauncherConfiguration defaultLauncherConfiguration() {
    return LauncherConfiguration.builder().launcherName("TEST_LAUNCHER").build();
  }

  private ProcessConfiguration defaultProcessConfiguration() {
    ProcessConfiguration processConfiguration =
        spy(ProcessConfiguration.builder().resolver(PROCESS_NAME).build());

    doReturn(resolver).when(processConfiguration).createResolver();

    return processConfiguration;
  }

  private TaskConfiguration defaultTaskConfiguration() {
    return TaskConfiguration.builder().retries(2).build();
  }

  private static final class TransientException extends RuntimeException {}

  private static final class PermanentException extends RuntimeException {}

  private static TaskExecutionResult success() {
    return resolver.success();
  }

  private static TaskExecutionResult transientError() {
    return resolver.resolveError(new TransientException());
  }

  private static TaskExecutionResult permanentError() {
    return resolver.resolveError(new PermanentException());
  }

  private static int successExitCode() {
    return resolver.exitCodeSerializer().serialize(resolver.success());
  }

  private static int transientErrorExitCode() {
    return resolver.exitCodeSerializer().serialize(resolver.resolveError(new TransientException()));
  }

  private static int permanentErrorExitCode() {
    return resolver.exitCodeSerializer().serialize(resolver.resolveError(new PermanentException()));
  }

  private static class MockStorage {
    public final PipeliteProcessService pipeliteProcessService = mock(PipeliteProcessService.class);
    public final PipeliteStageService pipeliteStageService = mock(PipeliteStageService.class);
    public final PipeliteLockService pipeliteLockService = new PipeliteInMemoryLockService();
  }

  private MockStorage mockStorage(
      final String[] names, final TaskExecutionResult[] init_results, final boolean[] enabled) {

    MockStorage mockStorage = new MockStorage();
    PipeliteProcessService pipeliteProcessService = mockStorage.pipeliteProcessService;
    PipeliteStageService pipeliteStageService = mockStorage.pipeliteStageService;

    final PipeliteProcess stored_state = new PipeliteProcess();
    stored_state.setProcessName(PROCESS_NAME);
    stored_state.setProcessId(PROCESS_ID);
    stored_state.setState(ProcessExecutionState.ACTIVE);
    stored_state.setExecutionCount(0);
    stored_state.setPriority(1);

    doAnswer(
            new Answer<Object>() {
              final AtomicInteger counter = new AtomicInteger();

              public Object answer(InvocationOnMock invocation) {
                PipeliteStage pipeliteStage = new PipeliteStage();
                pipeliteStage.setProcessId(PROCESS_ID);
                pipeliteStage.setStageName(names[counter.getAndAdd(1)]);
                pipeliteStage.setProcessName(PROCESS_NAME);
                pipeliteStage.setStartTime(LocalDateTime.now());
                pipeliteStage.setEndTime(LocalDateTime.now());
                pipeliteStage.setExecutionCount(0);
                pipeliteStage.setResultType(init_results[counter.get() - 1].getResultType());
                pipeliteStage.setResult(init_results[counter.get() - 1].getResult());

                pipeliteStage.setEnabled(enabled[counter.get() - 1]);
                if (counter.get() >= names.length) {
                  counter.set(0);
                }

                return Optional.of(pipeliteStage);
              }
            })
        .when(pipeliteStageService)
        .getSavedStage(any(), any(), any());

    doAnswer(
            (Answer<Object>)
                invocation -> {
                  PipeliteProcess pipeliteProcess = new PipeliteProcess();
                  pipeliteProcess.setProcessName(stored_state.getProcessName());
                  pipeliteProcess.setProcessId(stored_state.getProcessId());
                  pipeliteProcess.setState(stored_state.getState());
                  pipeliteProcess.setPriority(stored_state.getPriority());
                  pipeliteProcess.setExecutionCount(stored_state.getExecutionCount());
                  return Optional.of(pipeliteProcess);
                })
        .when(pipeliteProcessService)
        .getSavedProcess(any(), any());

    doAnswer(
            (Answer<Object>)
                invocation -> {
                  PipeliteProcess si = (PipeliteProcess) invocation.getArguments()[0];
                  stored_state.setProcessName(si.getProcessName());
                  stored_state.setProcessId(si.getProcessId());
                  stored_state.setState(si.getState());
                  stored_state.setPriority(si.getPriority());
                  stored_state.setExecutionCount(si.getExecutionCount());
                  return null;
                })
        .when(pipeliteProcessService)
        .saveProcess(any());

    return mockStorage;
  }

  private DefaultProcessLauncher initProcessLauncher(
      ProcessConfiguration processConfiguration, MockStorage mockStorage) {

    PipeliteProcess pipeliteProcess = new PipeliteProcess();
    pipeliteProcess.setProcessId(PROCESS_ID);
    pipeliteProcess.setProcessName(PROCESS_NAME);
    DefaultProcessLauncher process =
        spy(
            new DefaultProcessLauncher(
                defaultLauncherConfiguration(),
                processConfiguration,
                defaultTaskConfiguration(),
                mockStorage.pipeliteProcessService,
                mockStorage.pipeliteStageService,
                mockStorage.pipeliteLockService));
    process.init(PROCESS_ID);
    return process;
  }

  private TaskExecutor initExecutor(
      ProcessConfiguration processConfiguration, int... invocation_exit_code) {
    TaskExecutor spiedExecutor =
        spy(new InternalTaskExecutor(processConfiguration, mock(TaskConfiguration.class)));
    final AtomicInteger inv_cnt = new AtomicInteger(0);
    doAnswer(
            (Answer<Object>)
                i -> {
                  TaskInstance si = (TaskInstance) i.getArguments()[0];
                  log.atInfo().log(
                      "Calling execute on \"" + si.getPipeliteStage().getStageName() + "\"");
                  ExecutionInfo info = new ExecutionInfo();
                  info.setExitCode(
                      invocation_exit_code.length > inv_cnt.get()
                          ? invocation_exit_code[inv_cnt.getAndIncrement()]
                          : 0);
                  info.setThrowable(null);
                  info.setCommandline("Command Line");
                  info.setStderr("Stderr");
                  info.setStdout("Stdout");
                  return info;
                })
        .when(spiedExecutor)
        .execute(any(TaskInstance.class));

    return spiedExecutor;
  }

  private enum TestStages implements Stage {
    STAGE_1(null),
    STAGE_2(STAGE_1),
    STAGE_3(STAGE_2),
    STAGE_4(STAGE_3);

    private final TestStages dependsOn;

    public static class TestTask implements Task {
      @Override
      public void execute(TaskInstance taskInstance) {}
    }

    TestStages(TestStages dependsOn) {
      this.dependsOn = dependsOn;
    }

    @Override
    public String getStageName() {
      return this.name();
    }

    @Override
    public TaskFactory getTaskExecutorFactory() {
      return () -> new TestTask();
    }

    @Override
    public TestStages getDependsOn() {
      return dependsOn;
    }

    @Override
    public TaskConfiguration getTaskConfiguration() {
      return new TaskConfiguration();
    }
  }

  @Test
  public void Test() {

    Stage[] stages = TestStages.values();
    String[] names =
        Arrays.stream(TestStages.class.getEnumConstants()).map(Enum::name).toArray(String[]::new);

    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    doReturn(stages).when(processConfiguration).getStageArray();

    {
      MockStorage mockStorage =
          mockStorage(
              names,
              new TaskExecutionResult[] {success(), success(), transientError(), success()},
              new boolean[] {false, true, true, true});

      TaskExecutor taskExecutor =
          initExecutor(
              processConfiguration,
              successExitCode(),
              transientErrorExitCode(),
              successExitCode(),
              transientErrorExitCode());

      TaskExecutorFactory taskExecutorFactory =
          (processConfiguration1, taskConfiguration) -> taskExecutor;
      doReturn(taskExecutorFactory).when(processConfiguration).createExecutorFactory();

      DefaultProcessLauncher processLauncher =
          initProcessLauncher(processConfiguration, mockStorage);
      processLauncher.startAsync().awaitTerminated();

      verify(taskExecutor, times(2)).execute(any(TaskInstance.class));
      assertEquals(ProcessExecutionState.ACTIVE, processLauncher.getState());
      assertThat(processLauncher.getExecutionCount()).isEqualTo(1);
    }

    {
      MockStorage mockStorage =
          mockStorage(
              names,
              new TaskExecutionResult[] {permanentError(), success(), transientError(), success()},
              new boolean[] {false, false, true, true});

      TaskExecutor taskExecutor = initExecutor(processConfiguration);

      TaskExecutorFactory taskExecutorFactory =
          (processConfiguration1, taskConfiguration) -> taskExecutor;
      doReturn(taskExecutorFactory).when(processConfiguration).createExecutorFactory();

      DefaultProcessLauncher processLauncher =
          initProcessLauncher(processConfiguration, mockStorage);
      processLauncher.startAsync().awaitTerminated();

      verify(taskExecutor, times(2)).execute(any(TaskInstance.class));
    }

    {
      MockStorage mockStorage =
          mockStorage(
              names,
              new TaskExecutionResult[] {success(), success(), transientError(), success()},
              new boolean[] {false, true, true, true});

      TaskExecutor taskExecutor = initExecutor(processConfiguration);

      TaskExecutorFactory taskExecutorFactory =
          (processConfiguration1, taskConfiguration) -> taskExecutor;
      doReturn(taskExecutorFactory).when(processConfiguration).createExecutorFactory();

      DefaultProcessLauncher processLauncher =
          initProcessLauncher(processConfiguration, mockStorage);
      processLauncher.startAsync().awaitTerminated();

      verify(taskExecutor, times(2)).execute(any(TaskInstance.class));
    }

    {
      MockStorage mockStorage =
          mockStorage(
              names,
              new TaskExecutionResult[] {permanentError(), success(), transientError(), success()},
              new boolean[] {true, true, true, true});

      TaskExecutor taskExecutor = initExecutor(processConfiguration);

      TaskExecutorFactory taskExecutorFactory =
          (processConfiguration1, taskConfiguration) -> taskExecutor;
      doReturn(taskExecutorFactory).when(processConfiguration).createExecutorFactory();

      DefaultProcessLauncher processLauncher =
          initProcessLauncher(processConfiguration, mockStorage);
      processLauncher.startAsync().awaitTerminated();

      verify(taskExecutor, times(0)).execute(any(TaskInstance.class));
      assertEquals(ProcessExecutionState.FAILED, processLauncher.getState());
    }

    {
      MockStorage mockStorage =
          mockStorage(
              names,
              new TaskExecutionResult[] {transientError(), success(), transientError(), success()},
              new boolean[] {true, true, true, true});

      TaskExecutor taskExecutor = initExecutor(processConfiguration);

      TaskExecutorFactory taskExecutorFactory =
          (processConfiguration1, taskConfiguration) -> taskExecutor;
      doReturn(taskExecutorFactory).when(processConfiguration).createExecutorFactory();

      DefaultProcessLauncher processLauncher =
          initProcessLauncher(processConfiguration, mockStorage);
      processLauncher.startAsync().awaitTerminated();

      verify(taskExecutor, times(4)).execute(any(TaskInstance.class));
      assertEquals(ProcessExecutionState.COMPLETED, processLauncher.getState());
    }
  }
  */
}
