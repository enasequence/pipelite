package pipelite.launcher;

import pipelite.process.ProcessState;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PipeliteSchedulerCount {
  AtomicLong processFactoryNoProcessErrorCount = new AtomicLong(0);
  AtomicLong processExecutionNotUniqueProcessIdErrorCount = new AtomicLong(0);
  private Map<ProcessState, AtomicLong> processExecutionEndedCount = new ConcurrentHashMap<>();
  AtomicLong processExecutionExceptionCount = new AtomicLong(0);
  AtomicLong stageFailedCount = new AtomicLong(0);
  AtomicLong stageCompletedCount = new AtomicLong(0);

  public long getProcessFactoryNoProcessErrorCount() {
    return processFactoryNoProcessErrorCount.get();
  }

  public long getProcessExecutionNotUniqueProcessIdErrorCount() {
    return processExecutionNotUniqueProcessIdErrorCount.get();
  }

  public AtomicLong getProcessExecutionEndedCount(ProcessState state) {
    processExecutionEndedCount.putIfAbsent(state, new AtomicLong(0));
    return processExecutionEndedCount.get(state);
  }

  public long getProcessExecutionExceptionCount() {
    return processExecutionExceptionCount.get();
  }

  public long getStageFailedCount() {
    return stageFailedCount.get();
  }

  public long getStageCompletedCount() {
    return stageCompletedCount.get();
  }
}
