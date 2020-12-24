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
package pipelite.process;

import com.google.common.flogger.FluentLogger;
import pipelite.entity.ProcessEntity;

/** Creates new process instances for execution given process ids. */
public interface ProcessFactory {

  /**
   * Returns the pipeline name. The pipeline name must be unique.
   *
   * @return the pipeine name
   */
  String getPipelineName();

  /**
   * Returns the maximum number of parallel process executions for the pipeline.
   *
   * @return the maximum number of parallel process executions for the pipeline.
   */
  int getProcessParallelism();

  /**
   * Creates a new process instance for execution given process id.
   *
   * @param processId the process id
   * @return a new process instance for execution with the given process id
   */
  Process create(String processId);

  FluentLogger log = FluentLogger.forEnclosingClass();

  static Process create(ProcessEntity processEntity, ProcessFactory processFactory) {
    String processId = processEntity.getProcessId();
    log.atInfo().log("Creating process: %s", processId);
    // Create process.
    Process process = processFactory.create(processId);
    if (process == null) {
      log.atSevere().log("Failed to create process: %s", processId);
      return null;
    }
    process.setProcessEntity(processEntity);
    return process;
  }
}
