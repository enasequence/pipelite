package pipelite.process;

public interface ProcessSource {
  /** Returns the next new process instance to be executed. */
  ProcessInstance next();

  /** Confirm that the new process instance has been accepted. */
  void accept(ProcessInstance processInstance);

  /** Confirm that the new process instance has been rejected. */
  void reject(ProcessInstance processInstance);
}
