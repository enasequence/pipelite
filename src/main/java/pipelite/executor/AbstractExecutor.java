/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.reflect.TypeToken;
import org.springframework.util.Assert;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.ExecutorParameters;

/** Executes a stage. Must be serializable to json. */
public abstract class AbstractExecutor<T extends ExecutorParameters> implements StageExecutor<T> {

  @JsonIgnore private final TypeToken<T> executorParamsTypeToken = new TypeToken<T>(getClass()) {};

  @JsonIgnore
  private final Class<T> executorParamsType = (Class<T>) executorParamsTypeToken.getRawType();

  @JsonIgnore private T executorParams;
  @JsonIgnore private StageExecutorRequest request;

  @Override
  public Class<T> getExecutorParamsType() {
    return executorParamsType;
  }

  @Override
  public T getExecutorParams() {
    return executorParams;
  }

  @Override
  public void setExecutorParams(T executorParams) {
    this.executorParams = executorParams;
  }

  public StageExecutorRequest getRequest() {
    return request;
  }

  @Override
  public void prepareExecution(
      PipeliteServices pipeliteServices, String pipelineName, String processId, Stage stage) {
    Assert.notNull(pipeliteServices, "Missing pipelite services");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(processId, "Missing process id");
    Assert.notNull(stage, "Missing stage");

    request =
        StageExecutorRequest.builder()
            .pipelineName(pipelineName)
            .processId(processId)
            .stage(stage)
            .build();
  }
}
