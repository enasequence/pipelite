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

import static java.util.stream.Collectors.groupingBy;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ProcessEntityId;
import pipelite.exception.PipeliteProcessRetryException;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.repository.ProcessRepository;
import pipelite.runner.process.ProcessQueuePriorityPolicy;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Retryable(
    listeners = {"dataSourceRetryListener"},
    maxAttemptsExpression = "#{@dataSourceRetryConfiguration.getAttempts()}",
    backoff =
        @Backoff(
            delayExpression = "#{@dataSourceRetryConfiguration.getDelay()}",
            maxDelayExpression = "#{@dataSourceRetryConfiguration.getMaxDelay()}",
            multiplierExpression = "#{@dataSourceRetryConfiguration.getMultiplier()}"),
    exceptionExpression = "#{@dataSourceRetryConfiguration.recoverableException(#root)}")
@Flogger
public class ProcessService {

  private final ProcessQueuePriorityPolicy priorityPolicy;
  private final ProcessRepository processRepository;
  private final MailService mailService;

  @Autowired JdbcTemplate jdbcTemplate;

  public ProcessService(
      @Autowired AdvancedConfiguration advancedConfiguration,
      @Autowired ProcessRepository processRepository,
      @Autowired MailService mailService) {
    this.priorityPolicy = advancedConfiguration.getProcessQueuePriorityPolicy();
    this.processRepository = processRepository;
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
    return processRepository.findById(new ProcessEntityId(processId, pipelineName));
  }

  private <T> List<T> getProcesses(Stream<T> processes, int maxProcessCount) {
    return processes.limit(maxProcessCount).collect(Collectors.toList());
  }

  /**
   * Returns pending processes in priority order.
   *
   * @param pipelineName the pipeline name
   * @param maxProcessCount the maximum number of processes to return
   * @return the pending processes for a pipeline in priority order
   */
  public List<ProcessEntity> getPendingProcesses(String pipelineName, int maxProcessCount) {
    try (Stream<ProcessEntity> fifoProcessStream =
            processRepository.findAllByPipelineNameAndProcessStateOrderByCreateTimeAsc(
                pipelineName, ProcessState.PENDING);
        Stream<ProcessEntity> priorityProcessStream =
            processRepository.findAllByPipelineNameAndProcessStateOrderByPriorityDesc(
                pipelineName, ProcessState.PENDING)) {

      List<ProcessEntity> fifoProcesses = getProcesses(fifoProcessStream, maxProcessCount);
      List<ProcessEntity> priorityProcesses = getProcesses(priorityProcessStream, maxProcessCount);

      return getPendingProcesses(priorityPolicy, maxProcessCount, fifoProcesses, priorityProcesses);
    }
  }

  public static List<ProcessEntity> getPendingProcesses(
      ProcessQueuePriorityPolicy priorityPolicy,
      int maxProcessCount,
      List<ProcessEntity> fifoProcesses,
      List<ProcessEntity> priorityProcesses) {
    int fifoTarget = 0;
    switch (priorityPolicy) {
      case PRIORITY:
        fifoTarget = 0;
        break;
      case PREFER_PRIORITY:
        fifoTarget = maxProcessCount / 4;
        break;
      case BALANCED:
        fifoTarget = maxProcessCount / 2;
        break;
      case PREFER_FIFO:
        fifoTarget = maxProcessCount * 3 / 4;
        break;
      case FIFO:
        fifoTarget = maxProcessCount;
        break;
    }

    List<ProcessEntity> processes = new ArrayList<>(maxProcessCount);

    fifoProcesses.stream()
        .sequential()
        .limit(fifoTarget)
        .collect(Collectors.toCollection(() -> processes));

    // Make sure that each process is added only once
    HashSet<String> processesSet = new HashSet<>();
    processes.stream()
        .sequential()
        .map(p -> p.getProcessId())
        .collect(Collectors.toCollection(() -> processesSet));

    priorityProcesses.stream()
        .sequential()
        .filter(p -> !processesSet.contains(p.getProcessId()))
        .limit(maxProcessCount - processes.size())
        .collect(Collectors.toCollection(() -> processes));

    return processes;
  }

  /**
   * Returns active processes that are not locked in priority order.
   *
   * @param pipelineName the pipeline name
   * @param maxProcessCount the maximum number of processes to return
   * @return active processes that are not locked in priority order
   */
  public List<ProcessEntity> getUnlockedActiveProcesses(String pipelineName, int maxProcessCount) {
    try (Stream<ProcessEntity> processes =
        processRepository.findUnlockedActiveByPipelineNameOrderByPriorityDesc(
            pipelineName, ZonedDateTime.now())) {
      return getProcesses(processes, maxProcessCount);
    }
  }

  /**
   * Returns completed processes.
   *
   * @param pipelineName the pipeline name
   * @param maxProcessCount the maximum number of processes to return
   * @return the completed processes for a pipeline
   */
  public List<ProcessEntity> getCompletedProcesses(String pipelineName, int maxProcessCount) {
    try (Stream<ProcessEntity> processes =
        processRepository.findAllByPipelineNameAndProcessStateOrderByStartTimeDesc(
            pipelineName, ProcessState.COMPLETED)) {
      return getProcesses(processes, maxProcessCount);
    }
  }

  /**
   * Returns failed processes.
   *
   * @param pipelineName the pipeline name
   * @param maxProcessCount the maximum number of processes to return
   * @return the failed processes for a pipeline
   */
  public List<ProcessEntity> getFailedProcesses(String pipelineName, int maxProcessCount) {
    try (Stream<ProcessEntity> processes =
        processRepository.findAllByPipelineNameAndProcessStateOrderByPriorityDesc(
            pipelineName, ProcessState.FAILED)) {
      return getProcesses(processes, maxProcessCount);
    }
  }

  /**
   * Returns processes.
   *
   * @param pipelineName pipeline name
   * @param state optional process state
   * @param maxProcessCount the maximum number of processes to return
   * @return processes
   */
  public List<ProcessEntity> getProcesses(
      String pipelineName, ProcessState state, int maxProcessCount) {
    List<ProcessEntity> processes = new ArrayList<>();
    if (pipelineName == null) {
      return processes;
    }
    if (state != null) {
      processes.addAll(
          getProcesses(
              processRepository.findAllByPipelineNameAndProcessStateOrderByStartTimeDesc(
                  pipelineName, state),
              maxProcessCount));
    } else {
      processes.addAll(
          getProcesses(
              processRepository.findAllByPipelineNameOrderByStartTimeDesc(pipelineName),
              maxProcessCount));
    }
    return processes;
  }

  @Data
  private static class ProcessStateRow {
    String pipelineName;
    ProcessState processState;
    Long count;
  }

  @Data
  public static class ProcessStateSummary {
    String pipelineName;
    long pendingCount;
    long activeCount;
    long completedCount;
    long failedCount;
  }

  public List<ProcessStateSummary> getProcessStateSummary() {
    List<ProcessStateSummary> list = new ArrayList<>();
    String sql =
        "SELECT PIPELINE_NAME, STATE, COUNT(1) FROM PIPELITE2_PROCESS GROUP BY PIPELINE_NAME, STATE";

    Map<String, List<ProcessStateRow>> groupedByPipelineName =
        jdbcTemplate
            .query(
                sql,
                (rs, rowNum) -> {
                  ProcessState state = null;
                  try {
                    state = ProcessState.valueOf(rs.getString(2));
                  } catch (IllegalArgumentException ex) {
                    // Ignore unknown states
                  }
                  ProcessStateRow row = new ProcessStateRow();
                  row.setPipelineName(rs.getString(1));
                  row.setProcessState(state);
                  row.setCount(rs.getLong(3));
                  return row;
                })
            .stream()
            .collect(groupingBy(ProcessStateRow::getPipelineName));

    for (String pipelineName : groupedByPipelineName.keySet()) {
      Map<ProcessState, List<ProcessStateRow>> groupedByProcessState =
          groupedByPipelineName.get(pipelineName).stream()
              .collect(groupingBy(ProcessStateRow::getProcessState));

      ProcessStateSummary stateSummary = new ProcessStateSummary();
      stateSummary.setPipelineName(pipelineName);
      if (groupedByProcessState.containsKey(ProcessState.PENDING)) {
        stateSummary.setPendingCount(groupedByProcessState.get(ProcessState.PENDING).get(0).count);
      }
      if (groupedByProcessState.containsKey(ProcessState.ACTIVE)) {
        stateSummary.setActiveCount(groupedByProcessState.get(ProcessState.ACTIVE).get(0).count);
      }
      if (groupedByProcessState.containsKey(ProcessState.COMPLETED)) {
        stateSummary.setCompletedCount(
            groupedByProcessState.get(ProcessState.COMPLETED).get(0).count);
      }
      if (groupedByProcessState.containsKey(ProcessState.FAILED)) {
        stateSummary.setFailedCount(groupedByProcessState.get(ProcessState.FAILED).get(0).count);
      }
      list.add(stateSummary);
    }
    return list;
  }

  /**
   * Saves the process.
   *
   * @param processEntity the process
   * @return the saved process
   */
  public ProcessEntity saveProcess(ProcessEntity processEntity) {
    log.atFiner().log("Saving process: " + processEntity.toString());
    return processRepository.save(processEntity);
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
  public ProcessEntity startExecution(ProcessEntity processEntity) {
    processEntity.startExecution();
    return saveProcess(processEntity);
  }

  /**
   * Called when the process execution ends. Sets the process state and the execution end time.
   * Increases the process execution count. Saves the process.
   *
   * @param process the process
   * @param processState the process state
   */
  public ProcessEntity endExecution(Process process, ProcessState processState) {
    ProcessEntity processEntity = process.getProcessEntity();
    processEntity.endExecution(processState);
    ProcessEntity savedProcess = saveProcess(processEntity);
    mailService.sendProcessExecutionMessage(process);
    return savedProcess;
  }

  /**
   * Delete the process.
   *
   * @param processEntity the process
   */
  public void delete(ProcessEntity processEntity) {
    processRepository.delete(processEntity);
  }

  /**
   * Returns true if there is a process that has failed and can be retried.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return true if there is a process that has failed and can be retried
   * @throws PipeliteProcessRetryException if there is a process that can't be retried
   */
  public boolean isRetryProcess(String pipelineName, String processId) {
    Optional<ProcessEntity> processEntity = getSavedProcess(pipelineName, processId);
    if (!processEntity.isPresent()) {
      throw new PipeliteProcessRetryException(pipelineName, processId, "process does not exist");
    }
    if (processEntity.get().getProcessState() != ProcessState.FAILED) {
      throw new PipeliteProcessRetryException(pipelineName, processId, "process is not failed");
    }
    return true;
  }
}
