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
   * Validates the process before it is configured.
   *
   * @param processEntity the process entity
   * @param registeredPipeline the registered pipeline
   */
  private static void validateBeforeConfigure(
      ProcessEntity processEntity, RegisteredPipeline registeredPipeline) {
    if (processEntity == null) {
      throw new PipeliteException("Failed to create process. Missing process entity.");
    }

    if (registeredPipeline == null) {
      throw new PipeliteException("Failed to create process. Missing registered pipeline.");
    }

    String pipelineName = registeredPipeline.pipelineName();
    if (pipelineName == null) {
      throw new PipeliteException("Failed to create process. Missing pipeline name.");
    }

    String processId = processEntity.getProcessId();
    if (processId == null) {
      throw new PipeliteException("Failed to create process. Missing process id.");
    }

    if (!pipelineName.equals(processEntity.getPipelineName())) {
      throw new PipeliteException(
          "Failed to create "
              + pipelineName
              + " process "
              + processId
              + ". Conflicting pipeline name in process: "
              + processEntity.getPipelineName());
    }
  }

  /**
   * Creates a process with stages and assigns the process entity to it.
   *
   * @param processEntity the process entity
   * @param registeredPipeline the registered pipeline
   * @return the created process
   * @throws PipeliteException if the new process could not be created
   */
  public static Process create(ProcessEntity processEntity, RegisteredPipeline registeredPipeline) {
    validateBeforeConfigure(processEntity, registeredPipeline);

    String processId = processEntity.getProcessId();
    ProcessBuilder processBuilder = new ProcessBuilder(processId);

    // Create process with stages.
    registeredPipeline.configureProcess(processBuilder);
    Process process = processBuilder.build();

    // Set process entity.
    process.setProcessEntity(processEntity);
    return process;
  }

  /**
   * Creates a process without stages and assigns the process entity to it.
   *
   * @param processEntity the process entity
   * @param registeredPipeline the registered pipeline
   * @return the created process
   * @throws PipeliteException if the new process could not be created
   */
  public static Process createWithoutStages(
      ProcessEntity processEntity, RegisteredPipeline registeredPipeline) {
    validateBeforeConfigure(processEntity, registeredPipeline);

    String processId = processEntity.getProcessId();
    ProcessBuilder processBuilder = new ProcessBuilder(processId);

    // Create process without stages.
    Process process = processBuilder.buildWithoutStages();

    // Set process entity.
    process.setProcessEntity(processEntity);
    return process;
  }
}
