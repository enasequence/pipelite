package pipelite.service;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

/** Used from {@link Retryable} annotations. */
@Service
public class RetryService {

  /** The database retry policy will retry all exceptions except {@link DataAccessException}s
   * that are not either {@link TransientDataAccessException}s or
   * {@link RecoverableDataAccessException)s.
   */
  public boolean databaseRetryPolicy(Throwable throwable) {
    return !(throwable instanceof DataAccessException)
        || throwable instanceof TransientDataAccessException
        || throwable instanceof RecoverableDataAccessException;
  }
}
