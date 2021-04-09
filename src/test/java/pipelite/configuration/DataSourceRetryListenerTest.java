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
package pipelite.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithConfigurations;

@SpringBootTest(classes = PipeliteTestConfigWithConfigurations.class)
@ActiveProfiles({"test", "DataSourceRetryListenerTest"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class DataSourceRetryListenerTest {

  private static final String EXCEPTION_MESSAGE = "test";

  @Autowired private DataSourceRetryListener dataSourceRetryListener;
  @Autowired private TestServiceWithListenerAlwaysThrows testServiceWithListenerAlwaysThrows;
  @Autowired private TestServiceWithListenerNeverThrows testServiceWithListenerNeverThrows;

  @TestConfiguration
  @Profile("DataSourceRetryListenerTest")
  static class TestConfig {

    @Bean
    TestServiceWithListenerAlwaysThrows testServiceWithListenerAlwaysThrows() {
      return new TestServiceWithListenerAlwaysThrows();
    }

    @Bean
    TestServiceWithListenerNeverThrows testServiceWithListenerNeverThrows() {
      return new TestServiceWithListenerNeverThrows();
    }
  }

  public static class TestException extends RuntimeException {
    public TestException(String message) {
      super(message);
    }
  }

  public static class TestServiceWithListenerAlwaysThrows {
    private final AtomicInteger cnt = new AtomicInteger();

    @Retryable(
        listeners = {"dataSourceRetryListener"},
        maxAttempts = 10,
        backoff = @Backoff(delay = 1, maxDelay = 1, multiplier = 1))
    public void test() {
      cnt.incrementAndGet();
      throw new TestException(EXCEPTION_MESSAGE);
    }
  }

  public static class TestServiceWithListenerNeverThrows {
    private final AtomicInteger cnt = new AtomicInteger();

    @Retryable(
        listeners = {"dataSourceRetryListener"},
        maxAttempts = 10,
        backoff = @Backoff(delay = 1, maxDelay = 1, multiplier = 1))
    public void test() {
      cnt.incrementAndGet();
    }
  }

  @Test
  public void testServiceWithListenerAlwaysThrows() {
    AtomicInteger onErrorThrows = new AtomicInteger();
    AtomicInteger onErrorThrowsCorrect = new AtomicInteger();
    dataSourceRetryListener.setOnErrorThrowableLister(
        throwable -> {
          onErrorThrows.incrementAndGet();
          if (throwable instanceof TestException) {
            onErrorThrowsCorrect.incrementAndGet();
          }
        });

    AtomicInteger closeThrows = new AtomicInteger();
    AtomicInteger closeThrowsCorrect = new AtomicInteger();
    dataSourceRetryListener.setCloseThrowableLister(
        throwable -> {
          closeThrows.incrementAndGet();
          if (throwable instanceof TestException) {
            closeThrowsCorrect.incrementAndGet();
          }
        });

    Exception exception =
        assertThrows(TestException.class, () -> testServiceWithListenerAlwaysThrows.test());
    assertThat(exception.getMessage()).isEqualTo(EXCEPTION_MESSAGE);

    assertThat(onErrorThrows.get()).isEqualTo(10);
    assertThat(onErrorThrowsCorrect.get()).isEqualTo(10);
    assertThat(closeThrows.get()).isEqualTo(1);
    assertThat(closeThrowsCorrect.get()).isEqualTo(1);
  }

  @Test
  public void testServiceWithListenerNeverThrows() {
    AtomicInteger onErrorThrows = new AtomicInteger();
    AtomicInteger onErrorThrowsCorrect = new AtomicInteger();
    dataSourceRetryListener.setOnErrorThrowableLister(
        throwable -> {
          onErrorThrows.incrementAndGet();
          if (throwable instanceof TestException) {
            onErrorThrowsCorrect.incrementAndGet();
          }
        });

    AtomicInteger closeThrows = new AtomicInteger();
    AtomicInteger closeThrowsCorrect = new AtomicInteger();
    dataSourceRetryListener.setCloseThrowableLister(
        throwable -> {
          closeThrows.incrementAndGet();
          if (throwable instanceof TestException) {
            closeThrowsCorrect.incrementAndGet();
          }
        });

    testServiceWithListenerNeverThrows.test();

    assertThat(onErrorThrows.get()).isEqualTo(0);
    assertThat(onErrorThrowsCorrect.get()).isEqualTo(0);
    assertThat(closeThrows.get()).isEqualTo(0);
    assertThat(closeThrowsCorrect.get()).isEqualTo(0);
  }
}
