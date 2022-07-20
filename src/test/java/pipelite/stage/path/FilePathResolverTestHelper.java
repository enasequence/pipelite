/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.stage.path;

import static org.mockito.Mockito.when;

import org.mockito.Mockito;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

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
