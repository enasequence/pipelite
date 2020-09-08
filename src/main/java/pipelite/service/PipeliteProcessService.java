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
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteProcessId;
import pipelite.process.ProcessExecutionState;
import pipelite.repository.PipeliteProcessRepository;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class PipeliteProcessService {

  private final PipeliteProcessRepository repository;

  public PipeliteProcessService(@Autowired PipeliteProcessRepository repository) {
    this.repository = repository;
  }

  public Optional<PipeliteProcess> getSavedProcess(String processName, String processId) {
    return repository.findById(new PipeliteProcessId(processId, processName));
  }

  public List<PipeliteProcess> getNewProcesses(String processName) {
    return repository.findAllByProcessNameAndStateOrderByPriorityDesc(
        processName, ProcessExecutionState.NEW);
  }

  public List<PipeliteProcess> getActiveProcesses(String processName) {
    return repository.findAllByProcessNameAndStateOrderByPriorityDesc(
        processName, ProcessExecutionState.ACTIVE);
  }

  public List<PipeliteProcess> getCompletedProcesses(String processName) {
    return repository.findAllByProcessNameAndState(processName, ProcessExecutionState.COMPLETED);
  }

  public List<PipeliteProcess> getFailedProcesses(String processName) {
    return repository.findAllByProcessNameAndStateOrderByPriorityDesc(
        processName, ProcessExecutionState.FAILED);
  }

  public PipeliteProcess saveProcess(PipeliteProcess pipeliteProcess) {
    return repository.save(pipeliteProcess);
  }

  public void delete(PipeliteProcess pipeliteProcess) {
    repository.delete(pipeliteProcess);
  }
}
