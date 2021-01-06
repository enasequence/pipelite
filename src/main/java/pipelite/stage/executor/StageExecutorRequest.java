package pipelite.stage.executor;

import lombok.Builder;
import lombok.Value;
import org.springframework.util.Assert;
import pipelite.stage.Stage;

@Value
@Builder
public class StageExecutorRequest {
  private final String pipelineName;
  private final String processId;
  private final Stage stage;

  public StageExecutorRequest(String pipelineName, String processId, Stage stage) {
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(processId, "Missing process id");
    Assert.notNull(stage, "Missing stage");
    Assert.notNull(stage.getStageName(), "Missing stage name");
    this.pipelineName = pipelineName;
    this.processId = processId;
    this.stage = stage;
  }
}
