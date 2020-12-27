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
package pipelite.service;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ProcessEntityId;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.repository.ProcessRepository;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class ProcessService {

  private final ProcessRepository repository;
  private final MailService mailService;

  public ProcessService(
      @Autowired ProcessRepository repository, @Autowired MailService mailService) {
    this.repository = repository;
    this.mailService = mailService;
  }

  /**
   * Returns a saved process.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return the saved process
   */
  public Optional<ProcessEntity> getSavedProcess(String pipelineName, String processId) {
    return repository.findById(new ProcessEntityId(processId, pipelineName));
  }

  private <T> List<T> list(Stream<T> strm, int limit) {
    return strm.limit(limit).collect(Collectors.toList());
  }

  /**
   * Returns pending processes for a pipeline in priority order.
   *
   * @param pipelineName the pipeline name
   * @param limit the maximum number of processes to return
   * @return the pending processes for a pipeline in priority order
   */
  public List<ProcessEntity> getPendingProcesses(String pipelineName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findAllByPipelineNameAndStateOrderByPriorityDesc(
            pipelineName, ProcessState.PENDING)) {
      return list(strm, limit);
    }
  }

  /**
   * Returns active processes that are not locked for a pipeline in priority order.
   *
   * @param pipelineName the pipeline name
   * @param limit the maximum number of processes to return
   * @return the active processes that are not locked for a pipeline in priority order
   */
  public List<ProcessEntity> getAvailableActiveProcesses(String pipelineName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findAvailableActiveOrderByPriorityDesc(pipelineName, ZonedDateTime.now())) {
      return list(strm, limit);
    }
  }

  /**
   * Returns completed processes for a pipeline.
   *
   * @param pipelineName the pipeline name
   * @param limit the maximum number of processes to return
   * @return the completed processes for a pipeline
   */
  public List<ProcessEntity> getCompletedProcesses(String pipelineName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findAllByPipelineNameAndState(pipelineName, ProcessState.COMPLETED)) {
      return list(strm, limit);
    }
  }

  /**
   * Returns failed processes for a pipeline.
   *
   * @param pipelineName the pipeline name
   * @param limit the maximum number of processes to return
   * @return the failed processes for a pipeline
   */
  public List<ProcessEntity> getFailedProcesses(String pipelineName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findAllByPipelineNameAndStateOrderByPriorityDesc(
            pipelineName, ProcessState.FAILED)) {
      return list(strm, limit);
    }
  }

  /**
   * Returns processes given optional pipeline name, process id and process state.
   *
   * @param pipelineName optional pipeline name
   * @param processId optional process id
   * @param state optional process state
   * @param limit the maximum number of processes to return
   * @return processes given optional pipeline name, process id and process state
   */
  public List<ProcessEntity> getProcesses(
      String pipelineName, String processId, ProcessState state, int limit) {
    List<ProcessEntity> processes = new ArrayList<>();
    // pipelineName processId state
    // Y            Y         Y/N
    if (pipelineName != null && processId != null) {
      Optional<ProcessEntity> process = getSavedProcess(pipelineName, processId);
      if (process.isPresent() && (state == null || state.equals(process.get().getState()))) {
        processes.add(process.get());
      }
    }
    // pipelineName processId state
    // N            Y         Y/N
    else if (pipelineName == null && processId != null) {
      processes.addAll(
          list(
              repository
                  .findAllByProcessId(processId)
                  .filter(process -> state == null || state.equals(process.getState())),
              limit));
      // pipelineName processId state
      // Y            N         Y
    } else if (pipelineName != null && processId == null && state != null) {
      processes.addAll(list(repository.findAllByPipelineNameAndState(pipelineName, state), limit));
      // pipelineName processId state
      // Y            N         N
    } else if (pipelineName != null && processId == null && state == null) {
      processes.addAll(list(repository.findAllByPipelineName(pipelineName), limit));
      // pipelineName processId state
      // N            N         Y
    } else if (pipelineName == null && processId == null && state != null) {
      processes.addAll(list(repository.findAllByState(state), limit));
    }
    // pipelineName processId state
    // N            N         N
    else {
      processes.addAll(list(repository.findAllStream(), limit));
    }
    return processes;
  }

  /**
   * Saves the process.
   *
   * @param processEntity the process
   * @return the saved process
   */
  public ProcessEntity saveProcess(ProcessEntity processEntity) {
    return repository.save(processEntity);
  }

  /**
   * Creates and saves a new process.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param priority te process priority
   * @return the new process
   */
  public ProcessEntity createExecution(String pipelineName, String processId, Integer priority) {
    ProcessEntity processEntity = ProcessEntity.createExecution(pipelineName, processId, priority);
    return saveProcess(processEntity);
  }

  /**
   * Called when the process execution starts. Sets the process state to active and sets the
   * execution start time. Saves the process.
   *
   * @param processEntity the process
   */
  public void startExecution(ProcessEntity processEntity) {
    processEntity.startExecution();
    saveProcess(processEntity);
  }

  /**
   * Called when the process execution ends. Sets the process state and the execution end time.
   * Increases the process execution count. Saves the process.
   *
   * @param process the process
   * @param processState the process state
   */
  public void endExecution(Process process, ProcessState processState) {
    ProcessEntity processEntity = process.getProcessEntity();
    processEntity.endExecution(processState);
    saveProcess(processEntity);
    mailService.sendProcessExecutionMessage(process);
  }

  /**
   * Delete the process.
   *
   * @param processEntity the process
   */
  public void delete(ProcessEntity processEntity) {
    repository.delete(processEntity);
  }
}
