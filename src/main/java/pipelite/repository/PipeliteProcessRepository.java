package pipelite.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteProcessId;

@Repository
public interface PipeliteProcessRepository extends CrudRepository<PipeliteProcess, PipeliteProcessId> {

}

