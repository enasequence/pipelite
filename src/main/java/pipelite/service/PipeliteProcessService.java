package pipelite.service;

import pipelite.entity.PipeliteProcess;

import java.util.List;
import java.util.Optional;

public interface PipeliteProcessService {

  Optional<PipeliteProcess> getSavedProcess(String processName, String processId);

  List<PipeliteProcess> getActiveProcesses(String processName);

  PipeliteProcess saveProcess(PipeliteProcess pipeliteProcess);

  void delete(PipeliteProcess pipeliteProcess);
}
