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

import com.google.common.flogger.FluentLogger;
import java.util.*;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.entity.ProcessEntity;
import pipelite.log.LogKey;
import pipelite.process.ProcessSource;
import pipelite.service.*;

@Flogger
public class ProcessCreator {

  private final ProcessSource processSource;
  private final ProcessService processService;
  private final String pipelineName;

  public ProcessCreator(
      ProcessSource processSource, ProcessService processService, String pipelineName) {
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(pipelineName, "Missing pipeline name");
    this.processSource = processSource;
    this.processService = processService;
    this.pipelineName = pipelineName;
  }

  /**
   * Creates and saves new processes using a process source.
   *
   * @param processCnt the number or requested processes
   * @return the number of created processes
   */
  public int createProcesses(int processCnt) {
    int createCnt = 0;
    if (processSource == null) {
      return createCnt;
    }
    logContext(log.atInfo()).log("Creating new processes");
    while (processCnt-- > 0) {
      ProcessSource.NewProcess newProcess = processSource.next();
      if (newProcess == null) {
        return createCnt;
      }
      if (createProcess(newProcess) != null) {
        createCnt++;
      }
    }
    logContext(log.atInfo()).log("Created " + createCnt + " new processes");
    return createCnt;
  }

  /**
   * Creates and saves a new process.
   *
   * @param newProcess the new process from process source
   * @return the created process or null if it could not be created
   */
  public ProcessEntity createProcess(ProcessSource.NewProcess newProcess) {
    String processId = newProcess.getProcessId();
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
          processService.createExecution(pipelineName, trimmedProcessId, newProcess.getPriority());
      if (processEntity == null) {
        logContext(log.atInfo(), trimmedProcessId).log("Failed to create process");
        throw new RuntimeException("Failed to create process: " + trimmedProcessId);
      }
    }
    processSource.accept(processId);
    return processEntity;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String processId) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName).with(LogKey.PROCESS_ID, processId);
  }
}
