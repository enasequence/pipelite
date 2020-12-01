package pipelite.process;

import org.junit.jupiter.api.Test;
import pipelite.entity.ProcessEntity;
import pipelite.process.builder.ProcessBuilder;
import pipelite.launcher.ProcessLauncherStats;
import pipelite.stage.StageExecutionResultType;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessFactoryTest {

  private static String PIPELINE_NAME = "PIPELINE1";
  private static String PROCESS_ID = "PROCESS1";

  @Test
  public void createSuccess() {
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setProcessId(PROCESS_ID);
    ProcessFactory processFactory =
        new ProcessFactory() {
          @Override
          public String getPipelineName() {
            return PIPELINE_NAME;
          }

          @Override
          public Process create(String processId) {
            return new ProcessBuilder(processId)
                .execute("STAGE1")
                .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
                .build();
          }
        };
    ProcessLauncherStats stats = new ProcessLauncherStats();

    Process process = ProcessFactory.create(processEntity, processFactory, stats);
    assertThat(process).isNotNull();
    assertThat(process.getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(process.getProcessEntity()).isNotNull();
    assertThat(process.getProcessEntity()).isSameAs(processEntity);
    assertThat(stats.getProcessCreationFailedCount()).isZero();
  }

  @Test
  public void createFailed() {
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setProcessId(PROCESS_ID);
    ProcessFactory processFactory =
        new ProcessFactory() {
          @Override
          public String getPipelineName() {
            return PIPELINE_NAME;
          }

          @Override
          public Process create(String processId) {
            return null;
          }
        };
    ProcessLauncherStats stats = new ProcessLauncherStats();

    Process process = ProcessFactory.create(processEntity, processFactory, stats);
    assertThat(process).isNull();
    assertThat(stats.getProcessCreationFailedCount()).isOne();
  }
}
