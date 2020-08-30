package pipelite.service;

import pipelite.entity.PipeliteMonitor;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;

import java.util.List;
import java.util.Optional;

public interface PipeliteMonitorService {

  List<PipeliteMonitor> getActiveMonitors(String processName);

  Optional<PipeliteMonitor> getSavedMonitor(String processName, String processId, String stageName);

  PipeliteMonitor saveMonitor(PipeliteMonitor pipeliteMonitor);

  void delete(PipeliteMonitor pipeliteMonitor);
}
