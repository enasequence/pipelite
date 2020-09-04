package pipelite.executor.runner;

import lombok.Value;

@Value
public
class CommandRunnerResult {
  private final int exitCode;
  private final String stdout;
  private final String stderr;
}
