package pipelite.executor.task;

/**
 * Callback interface for a task executed using {@link RetryTask}.
 *
 * @param <Context> the execution context
 * @param <Result>> the result returned by the task
 * @param <Exception>> the exception that may be thrown
 */
public interface RetryTaskCallback<Context, Result, Exception extends Throwable> {

  /**
   * Execute a {@link RetryTask}.
   *
   * @param context the execution context
   * @return the result of a successful task
   * @throws Exception the exception that is thrown if execution fails
   */
  Result execute(Context context) throws Exception;
}
