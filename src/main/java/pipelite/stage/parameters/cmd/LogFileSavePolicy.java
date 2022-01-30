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
 * The policy for saving stage log files that contain the stdout and stderr output of the stage
 * execution in the pipelite database.
 */
public enum LogFileSavePolicy {
  /** Save the log file. */
  ALWAYS,
  /** Save the log file if the stage execution failed. */
  ERROR,
  /** Do not save the log file. */
  NEVER;

  /**
   * Returns true if the stage log file should be saved.
   *
   * @param policy the stage log file save policy. If null then defaults to SAVE_ERROR.
   * @param result the stage execution result
   * @return true if the stage log file should be save
   */
  public static boolean isSave(LogFileSavePolicy policy, StageExecutorResult result) {
    if (policy == null) {
      policy = LogFileSavePolicy.ERROR;
    }
    switch (policy) {
      case ALWAYS:
        return true;
      case ERROR:
        return result.isError();
      default:
        return false;
    }
  }
}
