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

import com.google.common.primitives.Ints;
import java.util.List;
import pipelite.exception.PipeliteException;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.executor.StageExecutorState;

public enum ErrorType {
  EXECUTION_ERROR,
  TIMEOUT_ERROR,
  LOST_ERROR,
  PERMANENT_ERROR,
  INTERNAL_ERROR;

  public static ErrorType from(StageExecutorResult result, List<Integer> permanentErrors) {
    StageExecutorState state = result.state();

    if (!state.isError()) {
      return null;
    }

    switch (state) {
      case EXECUTION_ERROR:
        {
          if (permanentErrors != null && !permanentErrors.isEmpty()) {
            String exitCode = result.attribute(StageExecutorResultAttribute.EXIT_CODE);
            if (exitCode != null) {
              Integer exitCodeInt = Ints.tryParse(exitCode);
              if (exitCodeInt != null) {
                if (permanentErrors.contains(exitCodeInt)) {
                  return ErrorType.PERMANENT_ERROR;
                }
              }
            }
          }
          return ErrorType.EXECUTION_ERROR;
        }
      case TIMEOUT_ERROR:
        return ErrorType.TIMEOUT_ERROR;
      case LOST_ERROR:
        return ErrorType.LOST_ERROR;
      case INTERNAL_ERROR:
        return ErrorType.INTERNAL_ERROR;
    }
    throw new PipeliteException("Unexpected stage executor state: " + state.name());
  }

  public boolean isPermanentError() {
    return this == TIMEOUT_ERROR || this == PERMANENT_ERROR;
  }
}
