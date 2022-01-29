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
package pipelite.stage.parameters.cmd;

import pipelite.stage.executor.StageExecutorResult;

/**
 * The retention policy for stage log files that contain the stdout and stderr output of the stage
 * execution.
 */
public enum LogFileRetentionPolicy {
  /** Delete stage log files after stage execution has completed. */
  DELETE_ALWAYS,
  /** Delete stage log files if stage execution has completed successfully. */
  DELETE_SUCCESS,
  /** Do not delete stage log files. */
  DELETE_NEVER;

  /**
   * Returns true if the stage log file should be deleted.
   *
   * @param policy the stage log retention policy
   * @param result the stage execution result
   * @return true if the stage log file should be deleted
   */
  public static boolean isDelete(LogFileRetentionPolicy policy, StageExecutorResult result) {
    return (policy == LogFileRetentionPolicy.DELETE_ALWAYS
            && (result.isSuccess() || result.isError()))
        || (policy == LogFileRetentionPolicy.DELETE_SUCCESS && result.isSuccess());
  }
}
