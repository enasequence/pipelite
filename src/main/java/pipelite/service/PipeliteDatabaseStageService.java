package pipelite.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.entity.PipeliteStage;
import pipelite.entity.PipeliteStageId;
import pipelite.repository.PipeliteStageRepository;

import java.util.Optional;

@Service
public class PipeliteDatabaseStageService implements PipeliteStageService {

  private final PipeliteStageRepository repository;

  public PipeliteDatabaseStageService(@Autowired PipeliteStageRepository repository) {
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
