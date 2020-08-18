package pipelite.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import pipelite.entity.PipeliteStage;
import pipelite.entity.PipeliteStageId;

@Repository
public interface PipeliteStageRepository extends CrudRepository<PipeliteStage, PipeliteStageId> {
}
