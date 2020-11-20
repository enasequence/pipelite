package pipelite.launcher;

import pipelite.process.ProcessState;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PipeliteSchedulerStats {

  AtomicLong processCreationFailedCount = new AtomicLong(0);
  private Map<ProcessState, AtomicLong> processExecutionCount = new ConcurrentHashMap<>();
  AtomicLong processExceptionCount = new AtomicLong(0);
  AtomicLong stageFailedCount = new AtomicLong(0);
  AtomicLong stageSuccessCount = new AtomicLong(0);

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
