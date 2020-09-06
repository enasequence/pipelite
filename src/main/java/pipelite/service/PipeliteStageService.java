package pipelite.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.PipeliteStage;
import pipelite.entity.PipeliteStageId;
import pipelite.repository.PipeliteStageRepository;

import java.util.Optional;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class PipeliteStageService {

  private final PipeliteStageRepository repository;

  public PipeliteStageService(@Autowired PipeliteStageRepository repository) {
    this.repository = repository;
  }

  public Optional<PipeliteStage> getSavedStage(
      String processName, String processId, String stageName) {
    return repository.findById(new PipeliteStageId(processId, processName, stageName));
  }

  public PipeliteStage saveStage(PipeliteStage pipeliteStage) {
    return repository.save(pipeliteStage);
  }

  public void delete(PipeliteStage pipeliteStage) {
    repository.delete(pipeliteStage);
  }
}
