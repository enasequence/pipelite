package uk.ac.ebi.ena.sra.pipeline.launcher;

import lombok.AllArgsConstructor;
import org.springframework.transaction.support.TransactionTemplate;
import pipelite.entity.PipeliteProcess;
import pipelite.instance.TaskInstance;
import pipelite.launcher.DefaultPipeliteLauncher;
import pipelite.service.*;
import pipelite.stage.DefaultStage;
import pipelite.stage.Stage;
import pipelite.stage.StageFactory;
import pipelite.task.Task;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class DefaultPipeliteLauncherTester {

  private final DefaultPipeliteLauncher defaultPipeliteLauncher;
  private final PipeliteProcessService pipeliteProcessService;
  private final TransactionTemplate transactionTemplate;

  private static final AtomicInteger processExecutionCount = new AtomicInteger();
  private static final Set<String> processExecutionSet = ConcurrentHashMap.newKeySet();
  private static final Set<String> processExcessExecutionSet = ConcurrentHashMap.newKeySet();

  private static final int PROCESS_COUNT = 100;
  private static final int TASK_EXECUTION_TIME = 10; // ms

  public static class TestStages implements StageFactory {
    @Override
    public Stage[] create() {
      Stage[] stages = {new DefaultStage("STAGE_1", TestTask.class)};
      return stages;
    }
  }

  public static class TestTask implements Task {
    @Override
    public void execute(TaskInstance taskInstance) {
      String processId = taskInstance.getPipeliteProcess().getProcessId();
      processExecutionCount.incrementAndGet();
      if (processExecutionSet.contains(processId)) {
        processExcessExecutionSet.add(processId);
      }
      else {
        processExecutionSet.add(processId);
      }
        try {
          Thread.sleep(TASK_EXECUTION_TIME);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
    }
  }

  public void test() {
    processExecutionCount.set(0);

    transactionTemplate.execute(
        status -> {
          for (int i = 0; i < PROCESS_COUNT; ++i) {
            PipeliteProcess pipeliteProcess = new PipeliteProcess();
            pipeliteProcess.setProcessId("Process" + i);
            pipeliteProcess.setProcessName(defaultPipeliteLauncher.getProcessName());
            pipeliteProcessService.saveProcess(pipeliteProcess);
          }
          return true;
        });

    defaultPipeliteLauncher.setStopIfEmpty();
    defaultPipeliteLauncher.setLaunchTimeoutMilliseconds(10);
    defaultPipeliteLauncher.execute();

    // Because of the eventual guarantee of process execution status propagation
    // it is possible that an active process will be selected again for potential
    // execution before it has been processed by the launcher. However, these
    // process executions will be safely declined.

    assertThat(processExcessExecutionSet).isEmpty();

    assertThat(processExecutionCount.get()).isEqualTo(PROCESS_COUNT);

    assertThat(processExecutionCount.get())
        .isEqualTo(defaultPipeliteLauncher.getLaunchedProcessCount());

    assertThat(processExecutionCount.get())
        .isEqualTo(defaultPipeliteLauncher.getCompletedProcessCount());

    assertThat(defaultPipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
  }
}
