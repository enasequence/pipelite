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

import lombok.extern.flogger.Flogger;
import pipelite.entity.ProcessEntity;
import pipelite.process.builder.ProcessBuilder;
import pipelite.exception.PipeliteException;

/** Creates processes using a process factory. */
@Flogger
public class ProcessFactoryHelper {

  private ProcessFactoryHelper() {}

  /**
   * Creates a process.
   *
   * @param processId the process id
   * @param processFactory the process factory
   * @return the process
   * @throws PipeliteException if the new process could not be created
   */
  public static Process create(String processId, ProcessFactory processFactory) {
    if (processId == null) {
      throw new PipeliteException("Failed to create process. Missing process id.");
    }

    if (processFactory == null) {
      throw new PipeliteException("Failed to create process. Missing process factory.");
    }

    String pipelineName = processFactory.getPipelineName();

    if (pipelineName == null) {
      throw new PipeliteException("Failed to create process. Missing pipeline name.");
    }

    try {
      log.atInfo().log("Creating %s process %s", pipelineName, processId);

      Process process = processFactory.create(new ProcessBuilder(processId));
      if (process == null) {
        throw new PipeliteException(
            "Failed to create "
                + pipelineName
                + " process "
                + processId
                + ". Factory returned a null process.");
      }
      return process;
    } catch (Exception ex) {
      throw new PipeliteException(
          "Failed to create " + pipelineName + " process " + processId + ". Unexpected exception.",
          ex);
    }
  }

  /**
   * Creates a process.
   *
   * @param processEntity the process entity
   * @param processFactory the process factory
   * @return the process
   * @throws PipeliteException if the new process could not be created
   */
  public static Process create(ProcessEntity processEntity, ProcessFactory processFactory) {
    if (processEntity == null) {
      throw new PipeliteException("Failed to create process. Missing process entity.");
    }

    String processId = processEntity.getProcessId();

    if (processId == null) {
      throw new PipeliteException("Failed to create process. Missing process id.");
    }

    if (processFactory == null) {
      throw new PipeliteException(
          "Failed to create process " + processId + ". Missing process factory.");
    }

    String pipelineName = processFactory.getPipelineName();

    if (pipelineName == null) {
      throw new PipeliteException(
          "Failed to create process " + processId + ". Missing pipeline name.");
    }

    if (!processFactory.getPipelineName().equals(processEntity.getPipelineName())) {
      throw new PipeliteException(
          "Failed to create "
              + pipelineName
              + " process "
              + processId
              + ". Conflicting pipeline from process: "
              + processEntity.getPipelineName());
    }

    Process process = create(processId, processFactory);
    process.setProcessEntity(processEntity);
    return process;
  }
}
