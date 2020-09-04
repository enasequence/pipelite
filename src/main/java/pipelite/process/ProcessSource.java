package pipelite.process;

import lombok.Value;

public interface ProcessSource {

  @Value
  class NewProcess {
    private final String processId;
    private final int priority;
  }

  /** Returns the next new process to be executed. */
  NewProcess next();

  /** Confirms that the new process has been accepted. */
  void accept(String processId);

  /** Confirms that the new process has been rejected. */
  void reject(String processId);
}
