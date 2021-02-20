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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ProcessEntityId;
import pipelite.entity.StageEntity;
import pipelite.entity.StageEntityId;
import pipelite.exception.PipeliteProcessStateChangeException;
import pipelite.launcher.dependency.DependencyResolver;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.repository.ProcessRepository;
import pipelite.repository.StageRepository;
import pipelite.stage.Stage;

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
public class ProcessService {

  private final ProcessRepository repository;
  private final StageRepository stageRepository;
  private final MailService mailService;

  @Autowired JdbcTemplate jdbcTemplate;

  public ProcessService(
      @Autowired ProcessRepository repository,
      @Autowired StageRepository stageRepository,
      @Autowired MailService mailService) {
    this.repository = repository;
    this.stageRepository = stageRepository;
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
   * Returns pending processes in priority order.
   *
   * @param pipelineName the pipeline name
   * @param limit the maximum number of processes to return
   * @return the pending processes for a pipeline in priority order
   */
  public List<ProcessEntity> getPendingProcesses(String pipelineName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findAllByPipelineNameAndProcessStateOrderByPriorityDesc(
            pipelineName, ProcessState.PENDING)) {
      return list(strm, limit);
    }
  }

  /**
   * Returns available (unlocked) active processes in priority order.
   *
   * @param pipelineName the pipeline name
   * @param limit the maximum number of processes to return
   * @return available (unlocked) active processes in priority order
   */
  public List<ProcessEntity> getAvailableActiveProcesses(String pipelineName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findAvailableActiveOrderByPriorityDesc(pipelineName, ZonedDateTime.now())) {
      return list(strm, limit);
    }
  }

  /**
   * Returns completed processes.
   *
   * @param pipelineName the pipeline name
   * @param limit the maximum number of processes to return
   * @return the completed processes for a pipeline
   */
  public List<ProcessEntity> getCompletedProcesses(String pipelineName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findAllByPipelineNameAndProcessState(pipelineName, ProcessState.COMPLETED)) {
      return list(strm, limit);
    }
  }

  /**
   * Returns failed processes.
   *
   * @param pipelineName the pipeline name
   * @param limit the maximum number of processes to return
   * @return the failed processes for a pipeline
   */
  public List<ProcessEntity> getFailedProcesses(String pipelineName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findAllByPipelineNameAndProcessStateOrderByPriorityDesc(
            pipelineName, ProcessState.FAILED)) {
      return list(strm, limit);
    }
  }

  /**
   * Returns processes.
   *
   * @param pipelineName optional pipeline name
   * @param state optional process state
   * @param limit the maximum number of processes to return
   * @return processes
   */
  public List<ProcessEntity> getProcesses(String pipelineName, ProcessState state, int limit) {
    List<ProcessEntity> processes = new ArrayList<>();
    // pipelineName state
    // Y            Y
    if (pipelineName != null && state != null) {
      processes.addAll(
          list(repository.findAllByPipelineNameAndProcessState(pipelineName, state), limit));
      // pipelineName state
      // Y            N
    } else if (pipelineName != null) {
      processes.addAll(list(repository.findAllByPipelineName(pipelineName), limit));
      // pipelineName state
      // N            Y
    } else if (state != null) {
      processes.addAll(list(repository.findAllByProcessState(state), limit));
    }
    // pipelineName state
    // N            N
    else {
      processes.addAll(list(repository.findAllStream(), limit));
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

  /**
   * Change process state by resetting failed stages.
   *
   * @param pipelineName the pipeline name
   * @param process the process
   * @throws PipeliteProcessStateChangeException if the process state can't be changed
   */
  public void retry(String pipelineName, Process process)
      throws PipeliteProcessStateChangeException {
    getSavedProcessForProcessStateChange(pipelineName, process, ProcessState.FAILED);
    List<Stage> resetStages = new ArrayList<>();
    for (Stage stage : process.getStages()) {
      getSavedStageForProcessStateChange(pipelineName, process, stage);
      if (DependencyResolver.isPermanentlyFailedStage(stage)) {
        resetStages.add(stage);
      }
    }
    resetStagesForProcessStateChange(pipelineName, process, resetStages);
  }

  /**
   * Change process state by resetting the given completed stage and any dependent stages.
   *
   * @param pipelineName the pipeline name
   * @param stageName the stage name
   * @param process the process
   * @throws PipeliteProcessStateChangeException if the process state can't be changed
   */
  public void rerun(String pipelineName, String stageName, Process process)
      throws PipeliteProcessStateChangeException {
    Optional<Stage> stage = process.getStage(stageName);
    if (!stage.isPresent()) {
      throw new PipeliteProcessStateChangeException(
          pipelineName, process.getProcessId(), "invalid stage name " + stageName);
    }
    getSavedProcessForProcessStateChange(pipelineName, process, ProcessState.COMPLETED);
    List<Stage> resetStages = new ArrayList<>();
    resetStages.add(stage.get());
    resetStages.addAll(DependencyResolver.getDependentStages(process.getStages(), stage.get()));
    resetStagesForProcessStateChange(pipelineName, process, resetStages);
  }

  private void getSavedProcessForProcessStateChange(
      String pipelineName, Process process, ProcessState expectedProcessState) {
    // Get process entity
    Optional<ProcessEntity> processEntity =
        repository.findById(new ProcessEntityId(process.getProcessId(), pipelineName));
    if (!processEntity.isPresent()) {
      throw new PipeliteProcessStateChangeException(
          pipelineName, process.getProcessId(), "process does not exists");
    }
    process.setProcessEntity(processEntity.get());

    // Check process state
    ProcessState processState = process.getProcessEntity().getProcessState();
    if (processState != expectedProcessState) {
      throw new PipeliteProcessStateChangeException(
          pipelineName,
          process.getProcessId(),
          "process is " + processState + " but should be " + expectedProcessState);
    }
  }

  private void getSavedStageForProcessStateChange(
      String pipelineName, Process process, Stage stage) {
    Optional<StageEntity> stageEntity =
        stageRepository.findById(
            new StageEntityId(process.getProcessId(), pipelineName, stage.getStageName()));

    if (!stageEntity.isPresent()) {
      throw new PipeliteProcessStateChangeException(
          pipelineName, process.getProcessId(), "unknown stage " + stage.getStageName());
    }
    stage.setStageEntity(stageEntity.get());
  }

  private void resetStagesForProcessStateChange(
      String pipelineName, Process process, List<Stage> stages) {
    if (stages.isEmpty()) {
      throw new PipeliteProcessStateChangeException(
          pipelineName, process.getProcessId(), "no stages to reset");
    }
    for (Stage stage : stages) {
      stage.getStageEntity().resetExecution();
      stageRepository.save(stage.getStageEntity());
    }
    startExecution(process.getProcessEntity());
  }
}
