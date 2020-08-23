package pipelite.service;

import org.springframework.stereotype.Service;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteProcessId;
import pipelite.process.state.ProcessExecutionState;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class PipeliteInMemoryProcessService implements PipeliteProcessService {

  private final Map<PipeliteProcessId, PipeliteProcess> pipeliteProcesses =
      new ConcurrentHashMap<>();

  public Optional<PipeliteProcess> getSavedProcess(String processName, String processId) {
    return Optional.ofNullable(pipeliteProcesses.get(new PipeliteProcessId(processId, processName)));
  }

  public List<PipeliteProcess> getActiveProcesses(String processName) {
    return pipeliteProcesses.values().stream()
        .filter(
            p ->
                p.getProcessName().equals(processName)
                    && p.getState().equals(ProcessExecutionState.ACTIVE))
        .sorted(Comparator.comparing(PipeliteProcess::getPriority).reversed())
        .collect(Collectors.toList());
  }

  public List<PipeliteProcess> getCompletedProcesses(String processName) {
    return pipeliteProcesses.values().stream()
        .filter(
            p ->
                p.getProcessName().equals(processName)
                    && p.getState().equals(ProcessExecutionState.COMPLETED))
        .collect(Collectors.toList());
  }

  public List<PipeliteProcess> getFailedProcesses(String processName) {
    return pipeliteProcesses.values().stream()
        .filter(
            p ->
                p.getProcessName().equals(processName)
                    && p.getState().equals(ProcessExecutionState.FAILED))
        .sorted(Comparator.comparing(PipeliteProcess::getPriority).reversed())
        .collect(Collectors.toList());
  }

  public PipeliteProcess saveProcess(PipeliteProcess pipeliteProcess) {
    pipeliteProcesses.put(
        new PipeliteProcessId(pipeliteProcess.getProcessId(), pipeliteProcess.getProcessName()),
        pipeliteProcess);
    return pipeliteProcess;
  }

  public void delete(PipeliteProcess pipeliteProcess) {
    pipeliteProcesses.remove(
        new PipeliteProcessId(pipeliteProcess.getProcessId(), pipeliteProcess.getProcessName()));
  }
}
