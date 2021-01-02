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
package pipelite.stage.executor;

import pipelite.stage.Stage;

/** Executes a stage. */
public interface StageExecutorCall {
  /**
   * Called repeatedly to execute the stage until it is not ACTIVE. Only asynchronous executions are
   * expected to return ACTIVE.
   *
   * @param pipelineName the pipeline name.
   * @param processId the process id.
   * @param stage the stage to be executed.
   * @return stage execution result
   */
  StageExecutorResult execute(String pipelineName, String processId, Stage stage);
}