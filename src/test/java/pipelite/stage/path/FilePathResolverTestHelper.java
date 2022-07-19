package pipelite.stage.path;

import org.mockito.Mockito;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

import static org.mockito.Mockito.when;

public class FilePathResolverTestHelper {

  public static StageExecutorRequest request(
      String pipelineName, String processId, String stageName, String user, String logDir) {
    SimpleLsfExecutorParameters params =
        SimpleLsfExecutorParameters.builder().user(user).logDir(logDir).build();

    Stage stage = Mockito.mock(Stage.class);
    StageExecutor executor = Mockito.mock(StageExecutor.class);

    when(stage.getStageName()).thenReturn(stageName);
    when(stage.getExecutor()).thenReturn(executor);
    when(executor.getExecutorParams()).thenReturn(params);

    StageExecutorRequest request = new StageExecutorRequest(pipelineName, processId, stage);
    return request;
  }
}
