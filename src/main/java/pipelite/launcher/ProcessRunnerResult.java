package pipelite.launcher;

import lombok.Value;

@Value
public class ProcessRunnerResult {
  private final long processExecutionCount;
  private final long processExceptionCount;
  private final long stageSuccessCount;
  private final long stageFailedCount;
}
