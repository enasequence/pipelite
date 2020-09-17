package ena.example;

import pipelite.executor.StageExecutor;
import pipelite.process.ProcessFactory;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;

public class ExampleProcessFactory implements ProcessFactory {

  private static final String PIPELINE_NAME = "exampleProcess";

  public static class TestExecutor implements StageExecutor {
    @Override
    public StageExecutionResult execute(Stage stage) {
      System.out.println(
          "running stage " + stage.getStageName() + " for id " + stage.getProcessId());
      return StageExecutionResult.success();
    }
  }

  @Override
  public String getPipelineName() {
    return PIPELINE_NAME;
  }

  @Override
  public Process create(String processId) {
    return new ProcessBuilder(PIPELINE_NAME, processId, 9)
        .execute("STAGE1")
        .with(new TestExecutor())
        .executeAfterPrevious("STAGE2")
        .with(new TestExecutor())
        .build();
  }
}
