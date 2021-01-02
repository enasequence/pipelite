package pipelite.stage.executor;

public enum InternalError {
  /** Job execution related internal error. */
  EXECUTE,
  /** Asynchronous job submission related internal error. */
  SUBMIT,
  /** Asynchronous job polling related internal error. */
  POLL,
  /** Asynchronous job termination related internal error. */
  TERMINATE,
  /** Job execution timeout. */
  TIMEOUT
}
