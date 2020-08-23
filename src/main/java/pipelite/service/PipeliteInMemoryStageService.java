package pipelite.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteProcessId;
import pipelite.entity.PipeliteStage;
import pipelite.entity.PipeliteStageId;
import pipelite.repository.PipeliteStageRepository;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PipeliteInMemoryStageService implements PipeliteStageService {

  private final Map<PipeliteStageId, PipeliteStage> pipeliteStages = new ConcurrentHashMap<>();

  public Optional<PipeliteStage> getSavedStage(
      String processName, String processId, String stageName) {
    return Optional.ofNullable(
        pipeliteStages.get(new PipeliteStageId(processId, processName, stageName)));
  }

  public PipeliteStage saveStage(PipeliteStage pipeliteStage) {
    pipeliteStages.put(
        new PipeliteStageId(
            pipeliteStage.getProcessId(),
            pipeliteStage.getProcessName(),
            pipeliteStage.getStageName()),
        pipeliteStage);
    return pipeliteStage;
  }

  public void delete(PipeliteStage pipeliteStage) {
    pipeliteStages.remove(
        new PipeliteStageId(
            pipeliteStage.getProcessId(),
            pipeliteStage.getProcessName(),
            pipeliteStage.getStageName()));
  }
}
