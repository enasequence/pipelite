package pipelite.executor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

@Value
public class EmptySyncStageExecutor implements StageExecutor {

  private final StageExecutionResultType resultType;

  @JsonCreator
  public EmptySyncStageExecutor(@JsonProperty("resultType") StageExecutionResultType resultType) {
    this.resultType = resultType;
  }

  @Override
  public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
    return new StageExecutionResult(resultType);
  }
}
