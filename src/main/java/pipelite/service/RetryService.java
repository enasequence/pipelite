/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
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
