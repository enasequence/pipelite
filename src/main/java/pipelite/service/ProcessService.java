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

  @Autowired JdbcTemplate jdbcTemplate;

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
   * Returns pending processes in priority order.
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
        repository.findAllByPipelineNameAndState(pipelineName, ProcessState.COMPLETED)) {
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
        repository.findAllByPipelineNameAndStateOrderByPriorityDesc(
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
      processes.addAll(list(repository.findAllByPipelineNameAndState(pipelineName, state), limit));
      // pipelineName state
      // Y            N
    } else if (pipelineName != null && state == null) {
      processes.addAll(list(repository.findAllByPipelineName(pipelineName), limit));
      // pipelineName state
      // N            Y
    } else if (pipelineName == null && state != null) {
      processes.addAll(list(repository.findAllByState(state), limit));
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
        "SELECT pipeline_name, state, count(1) FROM PIPELITE_PROCESS GROUP BY pipeline_name, state";

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
      Map<ProcessState, List<ProcessStateRow>> groupedByState =
          groupedByPipelineName.get(pipelineName).stream()
              .collect(groupingBy(ProcessStateRow::getProcessState));

      ProcessStateSummary stateSummary = new ProcessStateSummary();
      stateSummary.setPipelineName(pipelineName);
      if (groupedByState.containsKey(ProcessState.PENDING)) {
        stateSummary.setPendingCount(groupedByState.get(ProcessState.PENDING).get(0).count);
      }
      if (groupedByState.containsKey(ProcessState.ACTIVE)) {
        stateSummary.setActiveCount(groupedByState.get(ProcessState.ACTIVE).get(0).count);
      }
      if (groupedByState.containsKey(ProcessState.COMPLETED)) {
        stateSummary.setCompletedCount(groupedByState.get(ProcessState.COMPLETED).get(0).count);
      }
      if (groupedByState.containsKey(ProcessState.FAILED)) {
        stateSummary.setFailedCount(groupedByState.get(ProcessState.FAILED).get(0).count);
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
}
