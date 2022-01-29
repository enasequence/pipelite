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

public enum OutputFileRetentionPolicy {
  /** Delete stage output file after execution has completed. */
  DELETE_ALWAYS,
  /** Delete stage output file if execution has completed successfully. */
  DELETE_SUCCESS,
  /** Do not delete stage output file. */
  DELETE_NEVER;

  public static boolean isDelete(OutputFileRetentionPolicy policy, StageExecutorResult result) {
    return (policy == OutputFileRetentionPolicy.DELETE_ALWAYS
            && (result.isSuccess() || result.isError()))
        || (policy == OutputFileRetentionPolicy.DELETE_SUCCESS && result.isSuccess());
  }
}
