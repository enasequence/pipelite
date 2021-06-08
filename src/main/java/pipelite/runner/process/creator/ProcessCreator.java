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
import pipelite.log.LogKey;
import pipelite.service.ProcessService;

@Flogger
public class ProcessCreator {

  private final Pipeline pipeline;
  private final ProcessService processService;
  private final String pipelineName;

  public ProcessCreator(Pipeline pipeline, ProcessService processService) {
    Assert.notNull(pipeline, "Missing pipeline");
    Assert.notNull(processService, "Missing process service");
    this.pipeline = pipeline;
    this.processService = processService;
    this.pipelineName = pipeline.pipelineName();
    Assert.notNull(this.pipelineName, "Missing pipeline name");
  }

  /**
   * Creates and saves processes.
   *
   * @param processCnt the number of requested processes to create
   * @return the number of created processes
   */
  public int createProcesses(int processCnt) {
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
      if (createProcess(process) != null) {
        createCnt++;
      }
    }
    logContext(log.atInfo()).log("Created " + createCnt + " new processes");
    return createCnt;
  }

  /**
   * Creates and saves one process.
   *
   * @param process the next process
   * @return the created process or null if it could not be created
   */
  public ProcessEntity createProcess(Pipeline.Process process) {
    String processId = process.getProcessId();
    if (processId == null || processId.trim().isEmpty()) {
      logContext(log.atWarning()).log("New process does not have process id");
      return null;
    }
    ProcessEntity processEntity;
    String trimmedProcessId = processId.trim();
    Optional<ProcessEntity> savedProcessEntity =
        processService.getSavedProcess(pipelineName, trimmedProcessId);
    if (savedProcessEntity.isPresent()) {
      processEntity = savedProcessEntity.get();
      logContext(log.atWarning(), trimmedProcessId).log("Ignoring existing new process");
    } else {
      logContext(log.atInfo(), trimmedProcessId).log("Creating new process");
      processEntity =
          processService.createExecution(
              pipelineName, trimmedProcessId, process.getPriority().getInt());
      if (processEntity == null) {
        logContext(log.atSevere(), trimmedProcessId).log("Failed to create process");
        throw new RuntimeException("Failed to create process: " + trimmedProcessId);
      }
    }
    pipeline.confirmProcess(processId);
    return processEntity;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String processId) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName).with(LogKey.PROCESS_ID, processId);
  }
}
