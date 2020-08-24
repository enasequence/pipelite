package pipelite.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteProcessId;
import pipelite.process.ProcessExecutionState;

import java.util.List;

@Repository
public interface PipeliteProcessRepository
    extends CrudRepository<PipeliteProcess, PipeliteProcessId> {

  List<PipeliteProcess> findAllByProcessNameAndState(String processName, ProcessExecutionState state);

  List<PipeliteProcess> findAllByProcessNameAndStateOrderByPriorityDesc(String processName, ProcessExecutionState state);
}
