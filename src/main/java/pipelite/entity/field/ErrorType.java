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
package pipelite.entity.field;

import pipelite.exception.PipeliteException;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;

public enum ErrorType {
  EXECUTION_ERROR,
  TIMEOUT_ERROR,
  PERMANENT_ERROR,
  INTERNAL_ERROR;

  public boolean isExecutable() {
    return this != TIMEOUT_ERROR && this != PERMANENT_ERROR;
  }

  public static ErrorType from(StageExecutorState state) {
    if (!state.isError()) {
      return null;
    }
    switch (state) {
      case EXECUTION_ERROR:
        return ErrorType.EXECUTION_ERROR;
      case TIMEOUT_ERROR:
        return ErrorType.TIMEOUT_ERROR;
      case PERMANENT_ERROR:
        return ErrorType.PERMANENT_ERROR;
      case INTERNAL_ERROR:
        return ErrorType.INTERNAL_ERROR;
    }
    throw new PipeliteException("Unexpected stage executor state: " + state.name());
  }

  public static ErrorType from(StageExecutorResult result) {
    return from(result.state());
  }
}
