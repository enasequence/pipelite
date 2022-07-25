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

public enum StageState {
  PENDING,
  ACTIVE,
  SUCCESS,
  ERROR;

  public static StageState from(StageExecutorState state) {
    if (state.isError()) {
      return ERROR;
    }
    switch (state) {
      case SUBMITTED:
      case ACTIVE:
        return StageState.ACTIVE;
      case SUCCESS:
        return StageState.SUCCESS;
    }
    throw new PipeliteException("Unexpected stage executor state: " + state.name());
  }

  public static StageState from(StageExecutorResult result) {
    return from(result.state());
  }
}
