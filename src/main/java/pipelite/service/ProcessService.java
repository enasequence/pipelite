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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ProcessEntityId;
import pipelite.process.ProcessState;
import pipelite.repository.ProcessRepository;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class ProcessService {

  private final ProcessRepository repository;

  public ProcessService(@Autowired ProcessRepository repository) {
    this.repository = repository;
  }

  public Optional<ProcessEntity> getSavedProcess(String pipelineName, String processId) {
    return repository.findById(new ProcessEntityId(processId, pipelineName));
  }

  public List<ProcessEntity> getNewProcesses(String pipelineName) {
    return repository.findAllByPipelineNameAndStateOrderByPriorityDesc(
        pipelineName, ProcessState.NEW);
  }

  public List<ProcessEntity> getActiveProcesses(String pipelineName) {
    return repository.findAllByPipelineNameAndStateOrderByPriorityDesc(
        pipelineName, ProcessState.ACTIVE);
  }

  public List<ProcessEntity> getCompletedProcesses(String pipelineName) {
    return repository.findAllByPipelineNameAndState(pipelineName, ProcessState.COMPLETED);
  }

  public List<ProcessEntity> getFailedProcesses(String pipelineName) {
    return repository.findAllByPipelineNameAndStateOrderByPriorityDesc(
        pipelineName, ProcessState.FAILED);
  }

  public ProcessEntity saveProcess(ProcessEntity processEntity) {
    return repository.save(processEntity);
  }

  public void delete(ProcessEntity processEntity) {
    repository.delete(processEntity);
  }
}
