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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import pipelite.process.instance.ProcessInstance;
import pipelite.task.executor.TaskExecutor;
import pipelite.task.instance.TaskInstance;
import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import pipelite.process.state.ProcessExecutionState;
import pipelite.task.result.TaskExecutionResult;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;
import uk.ac.ebi.ena.sra.pipeline.resource.MemoryLocker;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;

public class ProcessLauncherTest {

  private static final String MOCKED_PIPELINE = "MOCKED PIPELINE";
  static final Logger log = Logger.getLogger(ProcessLauncherTest.class);

  private final static class TransientException extends RuntimeException {}
  private final static class PermanentException extends RuntimeException {}

  private final static TaskExecutionResultExceptionResolver resolver =
          TaskExecutionResultExceptionResolver.builder()
                  .transientError(TransientException.class, "TRANSIENT_ERROR")
                  .permanentError(PermanentException.class, "PERMANENT_ERROR")
                  .build();

  private static TaskExecutionResult success() {
    return resolver.success();
  }

  private static TaskExecutionResult transientError() {
    return resolver.resolveError(new TransientException());
  }

  private static TaskExecutionResult permanentError()  {
    return resolver.resolveError(new PermanentException());
  }

  private static int successExitCode () {
    return resolver.exitCodeSerializer().serialize(resolver.success());
  }

  private static int transientErrorExitCode () {
    return resolver.exitCodeSerializer().serialize(resolver.resolveError(new TransientException()));
  }

  private static int permanentErrorExitCode () {
    return resolver.exitCodeSerializer().serialize(resolver.resolveError(new PermanentException()));
  }

  @BeforeClass
  public static void setup() {
    PropertyConfigurator.configure("resource/test.log4j.properties");
  }

  private StorageBackend initStorage(
          final String[] names, final TaskExecutionResult[] init_results, final boolean[] enabled)
      throws StorageException {
    StorageBackend mockedStorage = mock(StorageBackend.class);
    final ProcessInstance stored_state = new ProcessInstance();
    stored_state.setPipelineName(MOCKED_PIPELINE);
    stored_state.setProcessId("YOBA-PROCESS");
    stored_state.setState(ProcessExecutionState.ACTIVE);
    stored_state.setExecutionCount(0);
    stored_state.setPriority(1);
    stored_state.setProcessComment("PSHPSH! ALO YOBA ETO TY?");

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
        .when(mockedStorage)
        .load(any(TaskInstance.class));

    doAnswer(
            (Answer<Object>) invocation -> {
              ProcessInstance si = (ProcessInstance) invocation.getArguments()[0];
              si.setPipelineName(stored_state.getPipelineName());
              si.setProcessId(stored_state.getProcessId());
              si.setState(stored_state.getState());
              si.setPriority(stored_state.getPriority());
              si.setExecutionCount(stored_state.getExecutionCount());
              si.setProcessComment(stored_state.getProcessComment());
              return null;
            })
        .when(mockedStorage)
        .load(any(ProcessInstance.class));

    doAnswer(
            (Answer<Object>) invocation -> {
              ProcessInstance si = (ProcessInstance) invocation.getArguments()[0];

              stored_state.setPipelineName(si.getPipelineName());
              stored_state.setProcessId(si.getProcessId());
              stored_state.setState(si.getState());
              stored_state.setPriority(si.getPriority());
              stored_state.setExecutionCount(si.getExecutionCount());
              stored_state.setProcessComment(si.getProcessComment());

              return null;
            })
        .when(mockedStorage)
        .save(any(ProcessInstance.class));

    return mockedStorage;
  }

  private ProcessLauncher initProcessLauncher(
          Stage[] stages, TaskExecutionResultExceptionResolver resolver, StorageBackend storage, TaskExecutor executor) {
    ProcessLauncher process = spy(new ProcessLauncher(resolver));
    process.setProcessID("TEST_PROCESS");
    process.setStorage(storage);
    process.setExecutor(executor);
    process.setStages(stages);
    return process;
  }

  private TaskExecutor initExecutor(TaskExecutionResultExceptionResolver resolver, int... invocation_exit_code) {
    TaskExecutor spiedExecutor = spy(new InternalStageExecutor(resolver));
    final AtomicInteger inv_cnt = new AtomicInteger(0);
    doAnswer(
            (Answer<Object>) i -> {
              TaskInstance si = (TaskInstance) i.getArguments()[0];
              log.info("Calling execute on \"" + si.getTaskName() + "\"");
              return null;
            })
        .when(spiedExecutor)
        .execute(any(TaskInstance.class));

    doAnswer(
            (Answer<Object>) i -> {
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

    {

      StorageBackend mockedStorage =
          initStorage(
              new String[] {
                "1: SOVSE MALI YOBA", "2: MALI YOBA", "3: BOLSHE YOBA", "4: OCHE BOLSHE YOBA"
              },
              new TaskExecutionResult[] {success(), success(), transientError(), success() },
              new boolean[] {false, true, true, true});

      TaskExecutor spiedExecutor = initExecutor(resolver, successExitCode(), transientErrorExitCode(), successExitCode(), transientErrorExitCode());
      ProcessLauncher pl =
          initProcessLauncher(stages, resolver, mockedStorage, spiedExecutor);
      pl.setLocker(new MemoryLocker());
      pl.setRedoCount(2);
      pl.lifecycle();

      verify(pl, times(1)).lifecycle();
      verify(spiedExecutor, times(2)).execute(any(TaskInstance.class));

      Assert.assertEquals(ProcessExecutionState.ACTIVE, pl.getProcessInstance().getState());
      Assert.assertEquals(1, pl.getProcessInstance().getExecutionCount());

      // Re-run
      pl.lifecycle();

      verify(pl, times(2)).lifecycle();
      verify(spiedExecutor, times(4)).execute(any(TaskInstance.class));
      Assert.assertEquals(ProcessExecutionState.FAILED, pl.getProcessInstance().getState());
      Assert.assertEquals(2, pl.getProcessInstance().getExecutionCount());
    }

    {


      StorageBackend mockedStorage =
          initStorage(
              new String[] {"SOVSE MALI YOBA", "MALI YOBA", "BOLSHE YOBA", "OCHE BOLSHE YOBA"},
              new TaskExecutionResult[] {permanentError(), success(), transientError(), success()},
              new boolean[] {false, false, true, true});

      TaskExecutor spiedExecutor = initExecutor(resolver);
      ProcessLauncher pl =
          initProcessLauncher(stages, resolver, mockedStorage, spiedExecutor);
      pl.setLocker(new MemoryLocker());
      pl.lifecycle();

      verify(pl, times(1)).lifecycle();
      verify(spiedExecutor, times(2)).execute(any(TaskInstance.class));
    }

    {
      StorageBackend mockedStorage =
          initStorage(
              new String[] {"SOVSE MALI YOBA", "MALI YOBA", "BOLSHE YOBA", "OCHE BOLSHE YOBA"},
              new TaskExecutionResult[] {success(), success(), transientError(), success()},
              new boolean[] {false, true, true, true});

      TaskExecutor spiedExecutor = initExecutor(resolver);
      ProcessLauncher pl =
          initProcessLauncher(stages, resolver, mockedStorage, spiedExecutor);
      pl.setLocker(new MemoryLocker());
      pl.lifecycle();

      verify(pl, times(1)).lifecycle();
      verify(spiedExecutor, times(2)).execute(any(TaskInstance.class));
    }

    {
      StorageBackend mockedStorage =
          initStorage(
              new String[] {"SOVSE MALI YOBA", "MALI YOBA", "BOLSHE YOBA", "OCHE BOLSHE YOBA"},
              new TaskExecutionResult[] {permanentError(), success(), transientError(), success()},
              new boolean[] {true, true, true, true});

      TaskExecutor spiedExecutor = initExecutor(resolver);
      ProcessLauncher pl =
          initProcessLauncher(stages, resolver, mockedStorage, spiedExecutor);
      pl.setLocker(new MemoryLocker());
      pl.lifecycle();

      verify(pl, times(1)).lifecycle();
      verify(spiedExecutor, times(0)).execute(any(TaskInstance.class));

      Assert.assertEquals(ProcessExecutionState.FAILED, pl.getProcessInstance().getState());
    }

    {
      StorageBackend mockedStorage =
          initStorage(
              new String[] {"SOVSE MALI YOBA", "MALI YOBA", "BOLSHE YOBA", "OCHE BOLSHE YOBA"},
              new TaskExecutionResult[] {transientError(), success(), transientError(), success()},
              new boolean[] {true, true, true, true});

      TaskExecutor spiedExecutor = initExecutor(resolver);
      ProcessLauncher pl =
          initProcessLauncher(stages, resolver, mockedStorage, spiedExecutor);
      pl.setLocker(new MemoryLocker());
      pl.lifecycle();

      verify(pl, times(1)).lifecycle();
      verify(spiedExecutor, times(4)).execute(any(TaskInstance.class));

      Assert.assertEquals(ProcessExecutionState.COMPLETED, pl.getProcessInstance().getState());
    }
  }
}
