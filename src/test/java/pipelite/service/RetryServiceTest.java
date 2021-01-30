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

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import pipelite.PipeliteTestConfiguration;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(
    classes = PipeliteTestConfiguration.class)
public class RetryServiceTest {

  @Configuration
  static class ContextConfiguration {

    @Bean TestService testServiceFiveAttempts() {
      return new TestService();
    }
  }

  public static class TestService {
    private final AtomicInteger cnt = new AtomicInteger();
    @Retryable(
            maxAttempts = 5,
            backoff = @Backoff(delay = 1000 /* 1s */, maxDelay = 60000 /* 1 minute */, multiplier = 1),
            exceptionExpression = "#{@retryService.databaseRetryPolicy(#root)}")
    public void test() {
      cnt.incrementAndGet();
      throw new RuntimeException();
    }

    public int getCnt() {
      return cnt.get();
    }
  }

  @Autowired private TestService testServiceExpFiveAttempts;

  @Test
  public void testServiceExpFiveAttempts() {
    assertThrows(RuntimeException.class, () -> testServiceExpFiveAttempts.test());
    assertThat(testServiceExpFiveAttempts.getCnt()).isEqualTo(5);
  }
}
