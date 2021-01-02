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
package pipelite.executor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.reflect.TypeToken;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.parameters.ExecutorParameters;

/** Executes a stage. Must be serializable to json. */
public abstract class AbstractExecutor<T extends ExecutorParameters> implements StageExecutor<T> {

  @JsonIgnore private final TypeToken<T> executorParamsTypeToken = new TypeToken<T>(getClass()) {};

  @JsonIgnore
  private final Class<T> executorParamsType = (Class<T>) executorParamsTypeToken.getRawType();

  @JsonIgnore private T executorParams;

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
}
