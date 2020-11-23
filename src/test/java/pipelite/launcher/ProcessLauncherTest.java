package pipelite.launcher;

import org.junit.jupiter.api.Test;
import pipelite.configuration.StageConfiguration;
import pipelite.entity.StageEntity;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;
import pipelite.stage.StageParameters;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.stage.StageExecutionResultType.*;

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

  private List<ProcessLauncher.StageExecution> createStageExecutions(
      StageExecutionResultType firstResultType,
      StageExecutionResultType secondResultType,
      int executions,
      int maximumRetries,
      int immediateRetries) {
    StageParameters stageParams =
        StageParameters.builder()
            .maximumRetries(maximumRetries)
            .immediateRetries(immediateRetries)
            .build();
    Process process =
        new ProcessBuilder("test")
            .execute("STAGE0", stageParams)
            .with((pipelineName, processId, stage) -> null)
            .execute("STAGE1", stageParams)
            .with((pipelineName, processId, stage) -> null)
            .build();
    List<ProcessLauncher.StageExecution> stageExecutions = new ArrayList<>();

    Stage firstStage = process.getStages().get(0);
    StageEntity firstStageEntity = new StageEntity();
    firstStageEntity.setResultType(firstResultType);
    firstStageEntity.setExecutionCount(executions);
    ProcessLauncher.StageExecution firstStageExecution =
        new ProcessLauncher.StageExecution(firstStage, firstStageEntity);
    for (int i = 0; i < executions; ++i) {
      firstStageExecution.incrementImmediateExecutionCount();
    }
    stageExecutions.add(firstStageExecution);

    Stage secondStage = process.getStages().get(0);
    StageEntity secondStageEntity = new StageEntity();
    secondStageEntity.setResultType(secondResultType);
    secondStageEntity.setExecutionCount(executions);
    ProcessLauncher.StageExecution secondStageExecution =
        new ProcessLauncher.StageExecution(secondStage, secondStageEntity);
    for (int i = 0; i < executions; ++i) {
      secondStageExecution.incrementImmediateExecutionCount();
    }
    stageExecutions.add(secondStageExecution);

    return stageExecutions;
  }

  private void evaluateProcessStateNoRetries(
      StageExecutionResultType firstResultType,
      StageExecutionResultType secondResultType,
      ProcessState state) {
    List<ProcessLauncher.StageExecution> stageExecutions =
        createStageExecutions(firstResultType, secondResultType, 1, 0, 0);
    assertThat(ProcessLauncher.evaluateProcessState(stageExecutions)).isEqualTo(state);
  }

  @Test
  public void evaluateProcessStateNoRetries() {
    evaluateProcessStateNoRetries(NEW, NEW, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(NEW, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(NEW, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(NEW, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(NEW, null, ProcessState.ACTIVE);

    evaluateProcessStateNoRetries(SUCCESS, NEW, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(SUCCESS, SUCCESS, ProcessState.COMPLETED);
    evaluateProcessStateNoRetries(SUCCESS, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(SUCCESS, ERROR, ProcessState.FAILED);
    evaluateProcessStateNoRetries(SUCCESS, null, ProcessState.ACTIVE);

    evaluateProcessStateNoRetries(ACTIVE, NEW, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ACTIVE, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ACTIVE, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ACTIVE, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ACTIVE, null, ProcessState.ACTIVE);

    evaluateProcessStateNoRetries(ERROR, NEW, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ERROR, SUCCESS, ProcessState.FAILED);
    evaluateProcessStateNoRetries(ERROR, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ERROR, ERROR, ProcessState.FAILED);
    evaluateProcessStateNoRetries(ERROR, null, ProcessState.ACTIVE);
  }

  private void evaluateProcessStateWithRetries(
      StageExecutionResultType firstResultType,
      StageExecutionResultType secondResultType,
      ProcessState state) {
    List<ProcessLauncher.StageExecution> stageExecutions =
        createStageExecutions(firstResultType, secondResultType, 1, 1, 1);
    assertThat(ProcessLauncher.evaluateProcessState(stageExecutions)).isEqualTo(state);
  }

  @Test
  public void evaluateProcessStateWithRetries() {
    evaluateProcessStateWithRetries(NEW, NEW, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(NEW, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(NEW, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(NEW, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(NEW, null, ProcessState.ACTIVE);

    evaluateProcessStateWithRetries(SUCCESS, NEW, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(SUCCESS, SUCCESS, ProcessState.COMPLETED);
    evaluateProcessStateWithRetries(SUCCESS, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(SUCCESS, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(SUCCESS, null, ProcessState.ACTIVE);

    evaluateProcessStateWithRetries(ACTIVE, NEW, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, null, ProcessState.ACTIVE);

    evaluateProcessStateWithRetries(ERROR, NEW, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, null, ProcessState.ACTIVE);
  }
}
