package pipelite.service;

import pipelite.entity.PipeliteStage;

import java.util.Optional;

public interface PipeliteStageService {

  Optional<PipeliteStage> getSavedStage(String processName, String processId, String stageName);

  PipeliteStage saveStage(PipeliteStage pipeliteStage);

  void delete(PipeliteStage pipeliteStage);
}
