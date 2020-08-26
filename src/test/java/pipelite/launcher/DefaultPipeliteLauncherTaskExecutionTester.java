package pipelite.launcher;

import lombok.AllArgsConstructor;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteProcess;
import pipelite.executor.TaskExecutor;
import pipelite.instance.TaskInstance;
import pipelite.stage.DefaultStage;
import pipelite.stage.Stage;
import pipelite.stage.StageFactory;
import uk.ac.ebi.ena.sra.pipeline.launcher.ExecutionInfo;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class DefaultPipeliteLauncherTaskExecutionTester {

  private final DefaultPipeliteLauncher defaultPipeliteLauncher;

  public static class TestStagesLastFails implements StageFactory {
    @Override
    public Stage[] create() {
      Stage stage1 = new DefaultStage("STAGE_1");
      Stage stage2 = new DefaultStage("STAGE_2", stage1);
      Stage stage3 = new DefaultStage("STAGE_3", stage2);
      Stage stage4 = new DefaultStage("STAGE_4", stage3);
      Stage[] stages = {stage1, stage2, stage3, stage4};
      return stages;
    }
  }

  public static class SuccessExecutor implements TaskExecutor {
    @Override
    public ExecutionInfo execute(TaskInstance taskInstance) {
      return new ExecutionInfo();
    }
  }

  public static class FailExecutor implements TaskExecutor {
    @Override
    public ExecutionInfo execute(TaskInstance taskInstance) {
      throw new RuntimeException();
    }
  }

  public List<PipeliteProcess> testProcessesToSave() {
    List<PipeliteProcess> pipeliteProcesses = new ArrayList<>();

    // Last fails
    {
      PipeliteProcess pipeliteProcess = new PipeliteProcess();
      pipeliteProcess.setProcessId(UniqueStringGenerator.randomProcessId());
      pipeliteProcess.setProcessName(defaultPipeliteLauncher.getProcessName());
      pipeliteProcesses.add(pipeliteProcess);
    }

    return pipeliteProcesses;
  }

  public void testTaskExecution() {

    defaultPipeliteLauncher.setShutdownPolicy(
        DefaultPipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    defaultPipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(defaultPipeliteLauncher);

    assertThat(defaultPipeliteLauncher.getProcessInitCount()).isEqualTo(1);
    assertThat(defaultPipeliteLauncher.getProcessCompletedCount()).isEqualTo(0);
    assertThat(defaultPipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(defaultPipeliteLauncher.getTaskCompletedCount()).isEqualTo(3);
    assertThat(defaultPipeliteLauncher.getTaskFailedCount()).isEqualTo(1);
    assertThat(defaultPipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }
}
