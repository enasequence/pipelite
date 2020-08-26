package pipelite.instance;

public interface ProcessInstanceFactory {
  /** Receive a new process instance to be executed. */
  ProcessInstance receive();

  /** Confirm that the new process instance has been received. */
  void confirm(ProcessInstance processInstance);

  /** Reject the new process instance. */
  void reject(ProcessInstance processInstance);

  /** Load an existing process instance that has been received before. */
  ProcessInstance load(String processId);
}
