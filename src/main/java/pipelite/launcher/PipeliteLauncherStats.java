package pipelite.launcher;

import pipelite.process.ProcessState;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PipeliteLauncherStats {

  final AtomicLong processIdMissingCount = new AtomicLong(0);
  final AtomicLong processIdNotUniqueCount = new AtomicLong(0);
  final AtomicLong processCreationFailedCount = new AtomicLong(0);
  private final Map<ProcessState, AtomicLong> processExecutionCount = new ConcurrentHashMap<>();
  final AtomicLong processExceptionCount = new AtomicLong(0);
  final AtomicLong stageFailedCount = new AtomicLong(0);
  final AtomicLong stageSuccessCount = new AtomicLong(0);

  public long getProcessIdMissingCount() {
    return processIdMissingCount.get();
  }

  public long getProcessIdNotUniqueCount() {
    return processIdNotUniqueCount.get();
  }

  public long getProcessCreationFailedCount() {
    return processCreationFailedCount.get();
  }

  AtomicLong setProcessExecutionCount(ProcessState state) {
    processExecutionCount.putIfAbsent(state, new AtomicLong(0));
    return processExecutionCount.get(state);
  }

  public long getProcessExecutionCount(ProcessState state) {
    if (processExecutionCount.get(state) == null) {
      return 0;
    }
    return processExecutionCount.get(state).get();
  }

  public long getProcessExceptionCount() {
    return processExceptionCount.get();
  }

  public long getStageFailedCount() {
    return stageFailedCount.get();
  }

  public long getStageSuccessCount() {
    return stageSuccessCount.get();
  }
}
