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
package pipelite.runner.process.creator;

import com.google.common.flogger.FluentLogger;
import java.util.Optional;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.Pipeline;
import pipelite.entity.ProcessEntity;
import pipelite.exception.PipeliteException;
import pipelite.log.LogKey;
import pipelite.service.ProcessService;

@Flogger
public class ProcessEntityCreator {

  private final Pipeline pipeline;
  private final ProcessService processService;
  private final String pipelineName;

  public ProcessEntityCreator(Pipeline pipeline, ProcessService processService) {
    Assert.notNull(pipeline, "Missing pipeline");
    Assert.notNull(processService, "Missing process service");
    this.pipeline = pipeline;
    this.processService = processService;
    this.pipelineName = pipeline.pipelineName();
    Assert.notNull(this.pipelineName, "Missing pipeline name");
  }

  /**
   * Creates and saves process entities.
   *
   * @param processCnt the number of process entities to create
   * @return the number of created process entities
   */
  public int create(int processCnt) {
    if (pipeline == null) {
      return 0;
    }
    int createCnt = 0;
    logContext(log.atInfo()).log("Creating new processes");
    while (processCnt-- > 0) {
      Pipeline.Process process = pipeline.nextProcess();
      if (process == null) {
        return createCnt;
      }
      ProcessEntity processEntity = create(processService, pipelineName, process);
      if (processEntity != null) {
        pipeline.confirmProcess(processEntity.getProcessId());
        createCnt++;
      }
    }
    logContext(log.atInfo()).log("Created " + createCnt + " new processes");
    return createCnt;
  }

  /**
   * Creates and saves a process entity.
   *
   * @param processService the process service
   * @param pipelineName the pipeline name
   * @param process the process for which the process entity is created
   * @return the created process entity or null if it could not be created
   */
  public static ProcessEntity create(
      ProcessService processService, String pipelineName, Pipeline.Process process) {
    String processId = process.getProcessId();
    if (processId == null || processId.trim().isEmpty()) {
      throw new PipeliteException("Failed to create process: missing process id");
    }
    ProcessEntity processEntity;
    String trimmedProcessId = processId.trim();
    Optional<ProcessEntity> savedProcessEntity =
        processService.getSavedProcess(pipelineName, trimmedProcessId);
    if (savedProcessEntity.isPresent()) {
      // Process entity already exists
      processEntity = savedProcessEntity.get();
    } else {
      processEntity =
          processService.createExecution(
              pipelineName, trimmedProcessId, process.getPriority().getInt());
      if (processEntity == null) {
        throw new RuntimeException("Failed to create process: " + trimmedProcessId);
      }
    }
    return processEntity;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName);
  }
}
