package pipelite.launcher;

import org.junit.jupiter.api.Test;
import pipelite.configuration.StageConfiguration;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageParameters;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessLauncherTest {

  private void assertMaximumRetries(Integer maximumRetries, int expectedMaximumRetries) {
    assertThat(
            ProcessLauncher.getMaximumRetries(
                Stage.builder()
                    .stageName("STAGE")
                    .executor((pipelineName, processId, stage) -> StageExecutionResult.success())
                    .stageParameters(
                        StageParameters.builder().maximumRetries(maximumRetries).build())
                    .build()))
        .isEqualTo(expectedMaximumRetries);
  }

  private void assertImmediateRetries(
      Integer immediateRetries, Integer maximumRetries, int expectedImmediateRetries) {
    assertThat(
            ProcessLauncher.getImmediateRetries(
                Stage.builder()
                    .stageName("STAGE")
                    .executor((pipelineName, processId, stage) -> StageExecutionResult.success())
                    .stageParameters(
                        StageParameters.builder()
                            .maximumRetries(maximumRetries)
                            .immediateRetries(immediateRetries)
                            .build())
                    .build()))
        .isEqualTo(expectedImmediateRetries);
  }

  @Test
  public void maximumRetries() {
    assertMaximumRetries(1, 1);
    assertMaximumRetries(5, 5);
    assertMaximumRetries(null, StageConfiguration.DEFAULT_MAX_RETRIES);
  }

  @Test
  public void immediateRetries() {
    assertImmediateRetries(3, 6, 3);
    assertImmediateRetries(3, 2, 2);
    assertImmediateRetries(3, 0, 0);
    assertImmediateRetries(
        null,
        StageConfiguration.DEFAULT_IMMEDIATE_RETRIES + 1,
        StageConfiguration.DEFAULT_IMMEDIATE_RETRIES);
    assertImmediateRetries(null, null, StageConfiguration.DEFAULT_IMMEDIATE_RETRIES);
  }
}
