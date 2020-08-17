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
import static org.mockito.Mockito.*;

import java.sql.Timestamp;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import pipelite.configuration.ProcessConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.lock.ProcessInstanceLocker;
import pipelite.repository.PipeliteProcessRepository;
import pipelite.resolver.ConcreteExceptionResolver;
import pipelite.task.executor.TaskExecutor;
import pipelite.task.instance.TaskInstance;
import pipelite.resolver.ExceptionResolver;
import pipelite.process.state.ProcessExecutionState;
import pipelite.task.result.TaskExecutionResult;
import pipelite.stage.Stage;
import pipelite.lock.ProcessInstanceMemoryLocker;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;

public class ProcessLauncherTest {

  private static final String MOCKED_PIPELINE = "MOCKED PIPELINE";
  static final Logger log = Logger.getLogger(ProcessLauncherTest.class);

  private static final class TransientException extends RuntimeException {}

  private static final class PermanentException extends RuntimeException {}

  private static final ExceptionResolver resolver =
      ConcreteExceptionResolver.builder()
          .transientError(TransientException.class, "TRANSIENT_ERROR")
          .permanentError(PermanentException.class, "PERMANENT_ERROR")
          .build();

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
    public StorageBackend storageBackend = mock(StorageBackend.class);
    public PipeliteProcessRepository pipeliteProcessRepository =
        mock(PipeliteProcessRepository.class);
  }

  private MockStorage mockStorage(
      final String[] names, final TaskExecutionResult[] init_results, final boolean[] enabled)
      throws StorageBackend.StorageException {

    MockStorage mockStorage = new MockStorage();
    StorageBackend storageBackend = mockStorage.storageBackend;
    PipeliteProcessRepository pipeliteProcessRepository = mockStorage.pipeliteProcessRepository;

    final PipeliteProcess stored_state = new PipeliteProcess();
    stored_state.setProcessName(MOCKED_PIPELINE);
    stored_state.setProcessId("YOBA-PROCESS");
    stored_state.setState(ProcessExecutionState.ACTIVE);
    stored_state.setExecutionCount(0);
    stored_state.setPriority(1);

    doAnswer(
            new Answer<Object>() {
              final AtomicInteger counter = new AtomicInteger();

              public Object answer(InvocationOnMock invocation) {
                TaskInstance si = (TaskInstance) invocation.getArguments()[0];
                si.setEnabled(true);
                si.setExecutionCount(0);
                si.setProcessId("YOBA-PROCESS");
                si.setTaskName(names[counter.getAndAdd(1)]);
                si.setProcessName(MOCKED_PIPELINE);
                si.getLatestTaskExecution().setStartTime(new Timestamp(System.currentTimeMillis()));
                si.getLatestTaskExecution().setEndTime(new Timestamp(System.currentTimeMillis()));
                si.getLatestTaskExecution().setTaskExecutionResult(init_results[counter.get() - 1]);

                si.setDependsOn(1 == counter.get() ? null : names[counter.get() - 2]);

                si.setEnabled(enabled[counter.get() - 1]);
                if (counter.get() >= names.length) counter.set(0);

                return null;
              }
            })
        .when(storageBackend)
        .load(any(TaskInstance.class));

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
        .when(pipeliteProcessRepository)
        .findById(any());

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
        .when(pipeliteProcessRepository)
        .save(any());

    return mockStorage;
  }

  private ProcessLauncher initProcessLauncher(
      ProcessConfiguration processConfiguration, MockStorage mockStorage, TaskExecutor executor) {
    String launcherName = "TEST";
    ProcessInstanceLocker locker = new ProcessInstanceMemoryLocker();
    ProcessLauncher process =
        spy(new ProcessLauncher(launcherName, resolver, locker, mockStorage.pipeliteProcessRepository));
    process.setProcessID("TEST_PROCESS");
    process.setStorage(mockStorage.storageBackend);
    process.setExecutor(executor);
    process.setStages(processConfiguration.getStageArray());
    return process;
  }

  private TaskExecutor initExecutor(
      ProcessConfiguration processConfiguration, int... invocation_exit_code) {
    TaskExecutor spiedExecutor = spy(new InternalStageExecutor(processConfiguration));
    final AtomicInteger inv_cnt = new AtomicInteger(0);
    doAnswer(
            (Answer<Object>)
                i -> {
                  TaskInstance si = (TaskInstance) i.getArguments()[0];
                  log.info("Calling execute on \"" + si.getTaskName() + "\"");
                  return null;
                })
        .when(spiedExecutor)
        .execute(any(TaskInstance.class));

    doAnswer(
            (Answer<Object>)
                i -> {
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
        .get_info();

    return spiedExecutor;
  }

  @Test
  public void Test() throws StorageException {

    Stage[] stages =
        new Stage[] {mock(Stage.class), mock(Stage.class), mock(Stage.class), mock(Stage.class)};

    ProcessConfiguration processConfiguration = mock(ProcessConfiguration.class);
    doReturn(stages).when(processConfiguration).getStageArray();
    doReturn(resolver).when(processConfiguration).createResolver();

    {
      MockStorage mockStorage =
          mockStorage(
              new String[] {
                "1: SOVSE MALI YOBA", "2: MALI YOBA", "3: BOLSHE YOBA", "4: OCHE BOLSHE YOBA"
              },
              new TaskExecutionResult[] {success(), success(), transientError(), success()},
              new boolean[] {false, true, true, true});

      StorageBackend mockedStorage = mockStorage.storageBackend;

      TaskExecutor spiedExecutor =
          initExecutor(
              processConfiguration,
              successExitCode(),
              transientErrorExitCode(),
              successExitCode(),
              transientErrorExitCode());
      ProcessLauncher pl = initProcessLauncher(processConfiguration, mockStorage, spiedExecutor);
      pl.setRedoCount(2);
      pl.lifecycle();

      verify(pl, times(1)).lifecycle();
      verify(spiedExecutor, times(2)).execute(any(TaskInstance.class));

      assertEquals(ProcessExecutionState.ACTIVE, pl.getPipeliteProcess().getState());
      assertThat(pl.getPipeliteProcess().getExecutionCount()).isEqualTo(1);

      // Re-run
      pl.lifecycle();

      verify(pl, times(2)).lifecycle();
      verify(spiedExecutor, times(4)).execute(any(TaskInstance.class));
      assertEquals(ProcessExecutionState.FAILED, pl.getPipeliteProcess().getState());
      assertThat(pl.getPipeliteProcess().getExecutionCount()).isEqualTo(2);
    }

    {
      MockStorage mockStorage =
          mockStorage(
              new String[] {"SOVSE MALI YOBA", "MALI YOBA", "BOLSHE YOBA", "OCHE BOLSHE YOBA"},
              new TaskExecutionResult[] {permanentError(), success(), transientError(), success()},
              new boolean[] {false, false, true, true});

      StorageBackend mockedStorage = mockStorage.storageBackend;

      TaskExecutor spiedExecutor = initExecutor(processConfiguration);
      ProcessLauncher pl = initProcessLauncher(processConfiguration, mockStorage, spiedExecutor);
      pl.lifecycle();

      verify(pl, times(1)).lifecycle();
      verify(spiedExecutor, times(2)).execute(any(TaskInstance.class));
    }

    {
      MockStorage mockStorage =
          mockStorage(
              new String[] {"SOVSE MALI YOBA", "MALI YOBA", "BOLSHE YOBA", "OCHE BOLSHE YOBA"},
              new TaskExecutionResult[] {success(), success(), transientError(), success()},
              new boolean[] {false, true, true, true});

      StorageBackend mockedStorage = mockStorage.storageBackend;

      TaskExecutor spiedExecutor = initExecutor(processConfiguration);
      ProcessLauncher pl = initProcessLauncher(processConfiguration, mockStorage, spiedExecutor);
      pl.lifecycle();

      verify(pl, times(1)).lifecycle();
      verify(spiedExecutor, times(2)).execute(any(TaskInstance.class));
    }

    {
      MockStorage mockStorage =
          mockStorage(
              new String[] {"SOVSE MALI YOBA", "MALI YOBA", "BOLSHE YOBA", "OCHE BOLSHE YOBA"},
              new TaskExecutionResult[] {permanentError(), success(), transientError(), success()},
              new boolean[] {true, true, true, true});

      StorageBackend mockedStorage = mockStorage.storageBackend;

      TaskExecutor spiedExecutor = initExecutor(processConfiguration);
      ProcessLauncher pl = initProcessLauncher(processConfiguration, mockStorage, spiedExecutor);
      pl.lifecycle();

      verify(pl, times(1)).lifecycle();
      verify(spiedExecutor, times(0)).execute(any(TaskInstance.class));

      assertEquals(ProcessExecutionState.FAILED, pl.getPipeliteProcess().getState());
    }

    {
      MockStorage mockStorage =
          mockStorage(
              new String[] {"SOVSE MALI YOBA", "MALI YOBA", "BOLSHE YOBA", "OCHE BOLSHE YOBA"},
              new TaskExecutionResult[] {transientError(), success(), transientError(), success()},
              new boolean[] {true, true, true, true});

      StorageBackend mockedStorage = mockStorage.storageBackend;

      TaskExecutor spiedExecutor = initExecutor(processConfiguration);
      ProcessLauncher pl = initProcessLauncher(processConfiguration, mockStorage, spiedExecutor);
      pl.lifecycle();

      verify(pl, times(1)).lifecycle();
      verify(spiedExecutor, times(4)).execute(any(TaskInstance.class));

      assertEquals(ProcessExecutionState.COMPLETED, pl.getPipeliteProcess().getState());
    }
  }
}
