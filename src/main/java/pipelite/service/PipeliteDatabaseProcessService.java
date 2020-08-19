package pipelite.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteProcessId;
import pipelite.process.state.ProcessExecutionState;
import pipelite.repository.PipeliteProcessRepository;

import java.util.List;
import java.util.Optional;

@Service
public class PipeliteDatabaseProcessService implements PipeliteProcessService {

  private final PipeliteProcessRepository repository;

  public PipeliteDatabaseProcessService(@Autowired PipeliteProcessRepository repository) {
    this.repository = repository;
  }

  public Optional<PipeliteProcess> getSavedProcess(String processName, String processId) {
    return repository.findById(new PipeliteProcessId(processId, processName));
  }

  public List<PipeliteProcess> getActiveProcesses(String processName) {
    return repository.findAllByProcessNameAndStateOrderByPriorityDesc(
        processName, ProcessExecutionState.ACTIVE);
  }

  public List<PipeliteProcess> getCompletedProcesses(String processName) {
    return repository.findAllByProcessNameAndStateOrderByPriorityDesc(
        processName, ProcessExecutionState.COMPLETED);
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
