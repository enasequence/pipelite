package pipelite.executor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

import java.util.concurrent.atomic.AtomicBoolean;

@Value
public class EmptyAsyncStageExecutor implements StageExecutor {
  private final StageExecutionResultType resultType;
  @JsonIgnore AtomicBoolean isExecuted = new AtomicBoolean();

  @JsonCreator
  public EmptyAsyncStageExecutor(@JsonProperty("resultType") StageExecutionResultType resultType) {
    this.resultType = resultType;
  }

  @Override
  public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
    if (!isExecuted.get()) {
      isExecuted.set(true);
      return StageExecutionResult.active();
    } else {
      return new StageExecutionResult(resultType);
    }
  }
}
