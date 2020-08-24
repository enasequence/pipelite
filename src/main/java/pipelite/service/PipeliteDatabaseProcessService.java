package pipelite.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteProcessId;
import pipelite.process.ProcessExecutionState;
import pipelite.repository.PipeliteProcessRepository;

import java.util.List;
import java.util.Optional;

@Service
@Profile("database")
@Transactional(propagation = Propagation.REQUIRES_NEW)
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
    return repository.findAllByProcessNameAndState(
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
