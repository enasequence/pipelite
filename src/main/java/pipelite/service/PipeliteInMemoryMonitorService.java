package pipelite.service;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import pipelite.entity.PipeliteMonitor;
import pipelite.entity.PipeliteMonitorId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Profile("memory")
public class PipeliteInMemoryMonitorService implements PipeliteMonitorService {

  private final Map<PipeliteMonitorId, PipeliteMonitor> pipeliteMonitors =
      new ConcurrentHashMap<>();

  public List<PipeliteMonitor> getActiveMonitors(String processName) {
    return pipeliteMonitors.values().stream()
        .filter(p -> p.getMonitorName().equals(processName))
        .collect(Collectors.toList());
  }

  public Optional<PipeliteMonitor> getSavedMonitor(
      String processName, String processId, String stageName) {
    return Optional.ofNullable(
        pipeliteMonitors.get(new PipeliteMonitorId(processId, processName, stageName)));
  }

  public PipeliteMonitor saveMonitor(PipeliteMonitor pipeliteMonitor) {
    pipeliteMonitors.put(
        new PipeliteMonitorId(
            pipeliteMonitor.getProcessId(),
            pipeliteMonitor.getProcessName(),
            pipeliteMonitor.getStageName()),
        pipeliteMonitor);
    return pipeliteMonitor;
  }

  public void delete(PipeliteMonitor pipeliteMonitor) {
    pipeliteMonitors.remove(
        new PipeliteMonitorId(
            pipeliteMonitor.getProcessId(),
            pipeliteMonitor.getProcessName(),
            pipeliteMonitor.getStageName()));
  }
}
