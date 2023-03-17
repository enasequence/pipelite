/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.retryable;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import pipelite.executor.cmd.CmdRunner;
import pipelite.stage.executor.StageExecutorResult;

public class RetryTest {

  private static final int ATTEMPTS = 3;
  private static final Retry RETRY = Retry.fixed(Duration.ofMillis(1), ATTEMPTS);

  @Test
  public void testRetryActionSuccess() {
    AtomicInteger cnt = new AtomicInteger();
    RETRY.execute(
        () -> {
          cnt.incrementAndGet();
          return null;
        });
    assertThat(cnt.get()).isEqualTo(1);
  }

  @Test
  public void testRetryActionThrows() {
    AtomicInteger cnt = new AtomicInteger();
    assertThrows(
        RuntimeException.class,
        () ->
            RETRY.execute(
                () -> {
                  cnt.incrementAndGet();
                  throw new RuntimeException();
                }));
    assertThat(cnt.get()).isEqualTo(ATTEMPTS);
  }

  private static class TestCmdRunner implements CmdRunner {
    private final Supplier<StageExecutorResult> result;
    private int executeCount = 0;

    public TestCmdRunner(Supplier<StageExecutorResult> result) {
      this.result = result;
    }

    @Override
    public StageExecutorResult execute(String cmd) {
      ++executeCount;
      return result.get();
    }

    @Override
    public void writeFile(String str, Path file) {}

    public int executeCount() {
      return executeCount;
    }
  }

  @Test
  public void testCmdRunnerSuccess() {
    TestCmdRunner testCmdRunner = new TestCmdRunner(() -> StageExecutorResult.success());
    RETRY.execute(testCmdRunner, "test");
    Assertions.assertThat(testCmdRunner.executeCount()).isEqualTo(1);
  }

  @Test
  public void testCmdRunnerError() {
    TestCmdRunner testCmdRunner = new TestCmdRunner(() -> StageExecutorResult.executionError());
    RETRY.execute(testCmdRunner, "test");
    Assertions.assertThat(testCmdRunner.executeCount()).isEqualTo(ATTEMPTS);
  }

  @Test
  public void testCmdRunnerThrows() {
    TestCmdRunner testCmdRunner =
        new TestCmdRunner(
            () -> {
              throw new RuntimeException();
            });
    assertThrows(RuntimeException.class, () -> RETRY.execute(testCmdRunner, "test"));
    Assertions.assertThat(testCmdRunner.executeCount()).isEqualTo(ATTEMPTS);
  }
}
