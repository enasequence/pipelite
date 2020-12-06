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

  public Optional<ProcessEntity> getSavedProcess(String pipelineName, String processId) {
    return repository.findById(new ProcessEntityId(processId, pipelineName));
  }

  private <T> List<T> list(Stream<T> strm, int limit) {
    return strm.limit(limit).collect(Collectors.toList());
  }

  public List<ProcessEntity> getPendingProcesses(String pipelineName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findAllByPipelineNameAndStateOrderByPriorityDesc(
            pipelineName, ProcessState.PENDING)) {
      return list(strm, limit);
    }
  }

  public List<ProcessEntity> getActiveProcesses(
      String pipelineName, String launcherName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findActiveOrderByPriorityDesc(pipelineName, launcherName)) {
      return list(strm, limit);
    }
  }

  public List<ProcessEntity> getCompletedProcesses(String pipelineName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findAllByPipelineNameAndState(pipelineName, ProcessState.COMPLETED)) {
      return list(strm, limit);
    }
  }

  public List<ProcessEntity> getFailedProcesses(String pipelineName, int limit) {
    try (Stream<ProcessEntity> strm =
        repository.findAllByPipelineNameAndStateOrderByPriorityDesc(
            pipelineName, ProcessState.FAILED)) {
      return list(strm, limit);
    }
  }

  public List<ProcessEntity> getPendingProcesses(String pipelineName) {
    return getPendingProcesses(pipelineName, Integer.MAX_VALUE);
  }

  public List<ProcessEntity> getActiveProcesses(String pipelineName, String launcherName) {
    return getActiveProcesses(pipelineName, launcherName, Integer.MAX_VALUE);
  }

  public List<ProcessEntity> getCompletedProcesses(String pipelineName) {
    return getCompletedProcesses(pipelineName, Integer.MAX_VALUE);
  }

  public List<ProcessEntity> getFailedProcesses(String pipelineName) {
    return getFailedProcesses(pipelineName, Integer.MAX_VALUE);
  }

  public ProcessEntity saveProcess(ProcessEntity processEntity) {
    return repository.save(processEntity);
  }

  public String getMaxProcessId(String pipelineName) {
    return repository.findMaxProcessId(pipelineName);
  }

  public ProcessEntity createExecution(String pipelineName, String processId, Integer priority) {
    ProcessEntity processEntity = ProcessEntity.createExecution(pipelineName, processId, priority);
    return saveProcess(processEntity);
  }

  public void startExecution(ProcessEntity processEntity) {
    processEntity.startExecution();
    saveProcess(processEntity);
  }

  public void endExecution(String pipelineName, Process process, ProcessState processState) {
    ProcessEntity processEntity = process.getProcessEntity();
    processEntity.endExecution(processState);
    saveProcess(processEntity);
    mailService.sendProcessExecutionMessage(pipelineName, process);
  }

  public void delete(ProcessEntity processEntity) {
    repository.delete(processEntity);
  }
}
