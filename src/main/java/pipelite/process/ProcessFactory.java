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
package pipelite.process;

import lombok.extern.flogger.Flogger;
import pipelite.RegisteredPipeline;
import pipelite.entity.ProcessEntity;
import pipelite.exception.PipeliteException;
import pipelite.process.builder.ProcessBuilder;

@Flogger
public class ProcessFactory {

  private ProcessFactory() {}

  /**
   * Creates a process.
   *
   * @param processId the process id
   * @param registeredPipeline the registered pipeline
   * @return the process
   * @throws PipeliteException if the new process could not be created
   */
  public static Process create(String processId, RegisteredPipeline registeredPipeline) {
    if (processId == null) {
      throw new PipeliteException("Failed to create process. Missing process id.");
    }

    if (registeredPipeline == null) {
      throw new PipeliteException("Failed to create process. Missing registered pipeline.");
    }

    String pipelineName = registeredPipeline.pipelineName();

    if (pipelineName == null) {
      throw new PipeliteException("Failed to create process. Missing pipeline name.");
    }

    try {
      log.atFine().log("Creating %s process %s", pipelineName, processId);

      ProcessBuilder processBuilder = new ProcessBuilder(processId);
      registeredPipeline.configureProcess(processBuilder);
      Process process = processBuilder.build();
      if (process == null) {
        throw new PipeliteException(
            "Failed to create "
                + pipelineName
                + " process "
                + processId
                + ". Pipeline returned a null process.");
      }
      return process;
    } catch (PipeliteException ex) {
      throw ex;
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
   * @param registeredPipeline the registered pipeline
   * @return the process
   * @throws PipeliteException if the new process could not be created
   */
  public static Process create(ProcessEntity processEntity, RegisteredPipeline registeredPipeline) {
    if (processEntity == null) {
      throw new PipeliteException("Failed to create process. Missing process entity.");
    }

    String processId = processEntity.getProcessId();

    if (processId == null) {
      throw new PipeliteException("Failed to create process. Missing process id.");
    }

    if (registeredPipeline == null) {
      throw new PipeliteException(
          "Failed to create process " + processId + ". Missing registered pipeline.");
    }

    String pipelineName = registeredPipeline.pipelineName();

    if (pipelineName == null) {
      throw new PipeliteException(
          "Failed to create process " + processId + ". Missing pipeline name.");
    }

    if (!registeredPipeline.pipelineName().equals(processEntity.getPipelineName())) {
      throw new PipeliteException(
          "Failed to create "
              + pipelineName
              + " process "
              + processId
              + ". Conflicting pipeline from process: "
              + processEntity.getPipelineName());
    }

    Process process = create(processId, registeredPipeline);
    process.setProcessEntity(processEntity);
    return process;
  }
}
