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
package pipelite.retryable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.RecoverableDataAccessException;
import pipelite.configuration.RetryableDataSourceConfiguration;

public class RetryableDataAccessActionTest {

  @Test
  public void testNoRetry() throws SQLException {
    RetryableDataSourceConfiguration configuration = new RetryableDataSourceConfiguration();
    RetryableDataAccessAction<String> retryable = new RetryableDataAccessAction<>(configuration);

    assertThat(retryable.execute(() -> "test1")).isEqualTo("test1");
  }

  @Test
  public void testRetryWithTransientDataAccessException() throws SQLException {
    RetryableDataSourceConfiguration configuration = new RetryableDataSourceConfiguration();
    RetryableDataAccessAction<String> retryable = new RetryableDataAccessAction<>(configuration);
    configuration.setDelay(Duration.ofMillis(1));

    AtomicInteger retryCount = new AtomicInteger(10);
    assertThat(
            retryable.execute(
                () -> {
                  if (retryCount.decrementAndGet() > 0) {
                    throw new QueryTimeoutException("test2");
                  }
                  return "test1";
                }))
        .isEqualTo("test1");
    assertThat(retryCount.get()).isEqualTo(0);
  }

  @Test
  public void testRetryWithRecoverableDataAccessException() throws SQLException {
    RetryableDataSourceConfiguration configuration = new RetryableDataSourceConfiguration();
    RetryableDataAccessAction<String> retryable = new RetryableDataAccessAction<>(configuration);
    configuration.setDelay(Duration.ofMillis(1));

    AtomicInteger retryCount = new AtomicInteger(10);
    assertThat(
            retryable.execute(
                () -> {
                  if (retryCount.decrementAndGet() > 0) {
                    throw new RecoverableDataAccessException("test2");
                  }
                  return "test1";
                }))
        .isEqualTo("test1");
    assertThat(retryCount.get()).isEqualTo(0);
  }

  @Test
  public void testRetryWithSQLTransientConnectionException() throws SQLException {
    RetryableDataSourceConfiguration configuration = new RetryableDataSourceConfiguration();
    RetryableDataAccessAction<String> retryable = new RetryableDataAccessAction<>(configuration);
    configuration.setDelay(Duration.ofMillis(1));

    AtomicInteger retryCount = new AtomicInteger(10);
    assertThat(
            retryable.execute(
                () -> {
                  if (retryCount.decrementAndGet() > 0) {
                    throw new SQLTransientConnectionException("test2");
                  }
                  return "test1";
                }))
        .isEqualTo("test1");
    assertThat(retryCount.get()).isEqualTo(0);
  }

  @Test
  public void testRetryWithSQLTransientException() throws SQLException {
    RetryableDataSourceConfiguration configuration = new RetryableDataSourceConfiguration();
    RetryableDataAccessAction<String> retryable = new RetryableDataAccessAction<>(configuration);
    configuration.setDelay(Duration.ofMillis(1));

    AtomicInteger retryCount = new AtomicInteger(10);
    assertThat(
            retryable.execute(
                () -> {
                  if (retryCount.decrementAndGet() > 0) {
                    throw new SQLTransientException("test2");
                  }
                  return "test1";
                }))
        .isEqualTo("test1");
    assertThat(retryCount.get()).isEqualTo(0);
  }

  @Test
  public void testRetryWithSQLRecoverableException() throws SQLException {
    RetryableDataSourceConfiguration configuration = new RetryableDataSourceConfiguration();
    RetryableDataAccessAction<String> retryable = new RetryableDataAccessAction<>(configuration);
    configuration.setDelay(Duration.ofMillis(1));

    AtomicInteger retryCount = new AtomicInteger(10);
    assertThat(
            retryable.execute(
                () -> {
                  if (retryCount.decrementAndGet() > 0) {
                    throw new SQLRecoverableException("test2");
                  }
                  return "test1";
                }))
        .isEqualTo("test1");
    assertThat(retryCount.get()).isEqualTo(0);
  }

  @Test
  public void testUnrecoverableException() {
    RetryableDataSourceConfiguration configuration = new RetryableDataSourceConfiguration();
    RetryableDataAccessAction<String> retryable = new RetryableDataAccessAction<>(configuration);

    AtomicInteger retryCount = new AtomicInteger(10);
    assertThatThrownBy(
            () ->
                retryable.execute(
                    () -> {
                      retryCount.decrementAndGet();
                      throw new RuntimeException("test1s");
                    }))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("test1");
    assertThat(retryCount.get()).isEqualTo(9);
  }

  @Test
  public void testRetryDurationExceeded() {
    RetryableDataSourceConfiguration configuration = new RetryableDataSourceConfiguration();
    configuration.setDelay(Duration.ofMillis(1));
    configuration.setDuration(Duration.ofMillis(10));

    RetryableDataAccessAction<String> retryable = new RetryableDataAccessAction<>(configuration);

    assertThatThrownBy(
            () ->
                retryable.execute(
                    () -> {
                      throw new RecoverableDataAccessException("test2");
                    }))
        .isInstanceOf(RecoverableDataAccessException.class)
        .hasMessageContaining("test2");
  }
}
