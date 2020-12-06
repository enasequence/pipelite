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
package pipelite.launcher;

import java.util.List;
import pipelite.process.Process;

public interface ProcessRunnerPool {

  /**
   * Runs a process.
   *
   * @param pipelineName the pipeline name
   * @param process the process
   * @param callbacks the process execution callbacks
   */
  void runProcess(String pipelineName, Process process, ProcessRunnerCallback callbacks);

  /**
   * Returns the number of active process runners.
   *
   * @return the number of active process runners
   */
  int getActiveProcessRunnerCount();

  /**
   * Returns the active process runners.
   *
   * @return the active process runners
   */
  List<ProcessRunner> getActiveProcessRunners();

  /**
   * Returns true if the pipeline is active.
   *
   * @param pipelineName the pipeline name
   * @return true if the pipeline is active.
   */
  boolean isPipelineActive(String pipelineName);

  /**
   * Returns true if the process is active.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return true if the process is active.
   */
  boolean isProcessActive(String pipelineName, String processId);

  /** Shuts down the process runner pool/ */
  void shutDown();
}
