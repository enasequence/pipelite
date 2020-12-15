package pipelite.example;

import org.springframework.stereotype.Component;
import pipelite.process.ProcessFactory;
import pipelite.process.builder.ProcessBuilder;
import pipelite.process.Process;
import pipelite.stage.StageExecutionResultType;

@Component
public class TestPipeline implements ProcessFactory {

  public static final String PIPELINE_NAME = "testPipeline";

  @Override
  public String getPipelineName() {
    return PIPELINE_NAME;
  }

  @Override
  public Process create(String processId) {
    return new ProcessBuilder(processId)
        .execute("STAGE")
        .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
        .build();
  }
}
