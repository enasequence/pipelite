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
package pipelite.stage.executor;

public enum StageExecutorState {
  /** The job has been submitted. */
  SUBMITTED,
  /** The job is active. */
  ACTIVE,
  /** The job has completed successfully. */
  SUCCESS,
  /** The job has completed with an execution error. */
  EXECUTION_ERROR,
  /** The job has completed with a timeout error. */
  TIMEOUT_ERROR,
  /** The job has been lost. */
  LOST_ERROR,
  /** The job has completed with an internal error. */
  INTERNAL_ERROR;

  public boolean isSuccess() {
    return this == SUCCESS;
  }

  public boolean isError() {
    return this == EXECUTION_ERROR
        || this == TIMEOUT_ERROR
        || this == LOST_ERROR
        || this == INTERNAL_ERROR;
  }

  public boolean isCompleted() {
    return isSuccess() || isError();
  }
}
