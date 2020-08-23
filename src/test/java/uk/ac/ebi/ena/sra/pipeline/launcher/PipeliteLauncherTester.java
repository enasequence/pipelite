package uk.ac.ebi.ena.sra.pipeline.launcher;

import lombok.AllArgsConstructor;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.launcher.PipeliteLauncher;
import pipelite.process.launcher.DefaultProcessLauncherFactory;
import pipelite.service.*;
import pipelite.stage.DefaultStage;
import pipelite.stage.Stage;
import pipelite.stage.StageFactory;
import pipelite.task.Task;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class PipeliteLauncherTester {

  private LauncherConfiguration launcherConfiguration;
  private ProcessConfiguration processConfiguration;
  private TaskConfiguration taskConfiguration;
  private PipeliteProcessService pipeliteProcessService;
  private PipeliteStageService pipeliteStageService;
  private PipeliteLockService pipeliteLockService;

  private static final int PIPELITE_PROCESS_COUNT = 100;
  private static final int TASK_EXECUTION_TIME = 1; // ms

  private static AtomicInteger pipeliteProcessExecutionCount = new AtomicInteger(0);

  public static class TestStages implements StageFactory {
    @Override
    public Stage[] create() {
      Stage[] stages = {new DefaultStage("STAGE_1", TestTask.class)};
      return stages;
    }
  }

  public static class TestTask implements Task {
    @Override
    public void run() {
      pipeliteProcessExecutionCount.incrementAndGet();
      try {
        Thread.sleep(TASK_EXECUTION_TIME);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void test() {

    for (int i = 0; i < PIPELITE_PROCESS_COUNT; ++i) {
      PipeliteProcess pipeliteProcess = new PipeliteProcess();
      pipeliteProcess.setProcessId("Process" + i);
      pipeliteProcess.setProcessName(processConfiguration.getProcessName());
      pipeliteProcessService.saveProcess(pipeliteProcess);
    }

    PipeliteLauncher pipeliteLauncher =
        new PipeliteLauncher(
            launcherConfiguration,
            processConfiguration,
            pipeliteProcessService,
            new DefaultProcessLauncherFactory(
                launcherConfiguration,
                processConfiguration,
                taskConfiguration,
                pipeliteProcessService,
                pipeliteStageService,
                pipeliteLockService));
    pipeliteLauncher.stopIfEmpty();
    pipeliteLauncher.setLaunchTimeoutMilliseconds(100);
    pipeliteLauncher.execute();

    // Because of the eventual guarantee of process execution status propagation
    // it is possible that an active process will be selected again for potential
    // execution before it has been processed by the launcher. However, these
    // process executions will be safely declined.

    assertThat(pipeliteProcessExecutionCount.get()).isEqualTo(PIPELITE_PROCESS_COUNT);

    assertThat(pipeliteProcessExecutionCount.get())
        .isEqualTo(
            pipeliteLauncher.getLaunchedProcessCount()
                - pipeliteLauncher.getDeclinedProcessCount());

    assertThat(pipeliteProcessExecutionCount.get())
        .isEqualTo(
            pipeliteLauncher.getCompletedProcessCount()
                - pipeliteLauncher.getDeclinedProcessCount());

    assertThat(pipeliteLauncher.getLaunchedProcessCount())
        .isGreaterThanOrEqualTo(PIPELITE_PROCESS_COUNT);
    assertThat(pipeliteLauncher.getCompletedProcessCount())
        .isGreaterThanOrEqualTo(PIPELITE_PROCESS_COUNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
  }
}
