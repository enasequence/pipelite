package pipelite.launcher;

import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

@Data
public class ProcessRunnerResult {
  private long processExecutionCount;
  private long processExceptionCount;
  private AtomicLong stageSuccessCount = new AtomicLong();
  private AtomicLong stageFailedCount = new AtomicLong();
  private AtomicLong stageExceptionCount = new AtomicLong();

  public void addStageSuccessCount(long count) {
    this.stageSuccessCount.addAndGet(count);
  }

  public void addStageFailedCount(long count) {
    this.stageFailedCount.addAndGet(count);
  }

  public void addStageExceptionCount(long count) {
    this.stageExceptionCount.addAndGet(count);
  }

  public long getStageSuccessCount() {
    return stageSuccessCount.get();
  }

  public long getStageFailedCount() {
    return stageFailedCount.get();
  }

  public long getStageExceptionCount() {
    return stageFailedCount.get();
  }
}
