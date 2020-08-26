package pipelite.launcher;

import lombok.AllArgsConstructor;

import pipelite.configuration.ProcessConfigurationEx;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.entity.PipeliteProcess;
import pipelite.instance.TaskInstance;
import pipelite.service.PipeliteProcessService;
import pipelite.stage.DefaultStage;
import pipelite.stage.Stage;
import pipelite.task.Task;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class DefaultPipeliteLauncherTester {

  private final DefaultPipeliteLauncher defaultPipeliteLauncher;
  private final ProcessConfigurationEx processConfiguration;
  private final TaskConfigurationEx taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;

  private final AtomicInteger processExecutionCount = new AtomicInteger();
  private final Set<String> processExecutionSet = ConcurrentHashMap.newKeySet();
  private final Set<String> processExcessExecutionSet = ConcurrentHashMap.newKeySet();
  private static final int PROCESS_COUNT = 100;
  private static final int TASK_EXECUTION_TIME = 10; // ms

  private class TestTask implements Task {
    @Override
    public void execute(TaskInstance taskInstance) {
      String processId = taskInstance.getPipeliteProcess().getProcessId();
      processExecutionCount.incrementAndGet();
      if (processExecutionSet.contains(processId)) {
        processExcessExecutionSet.add(processId);
      } else {
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

    Stage[] stages = {new DefaultStage("STAGE_1", (taskInfo -> new TestTask()))};
    processConfiguration.setStages(stages);

    for (int i = 0; i < PROCESS_COUNT; ++i) {
      PipeliteProcess pipeliteProcess = new PipeliteProcess();
      pipeliteProcess.setProcessId("Process" + i);
      pipeliteProcess.setProcessName(defaultPipeliteLauncher.getProcessName());
      pipeliteProcessService.saveProcess(pipeliteProcess);
    }

    defaultPipeliteLauncher.setShutdownPolicy(
        DefaultPipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    defaultPipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(defaultPipeliteLauncher);

    assertThat(processExcessExecutionSet).isEmpty();
    assertThat(processExecutionCount.get()).isEqualTo(PROCESS_COUNT);
    assertThat(processExecutionCount.get())
        .isEqualTo(defaultPipeliteLauncher.getProcessCompletedCount());
    assertThat(defaultPipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
  }
}
