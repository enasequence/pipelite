package pipelite.executor.lsf;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AbstractLsfExecutorTest {

  @Test
  public void testExtractJobIdSubmitted() {
    assertThat(
            AbstractLsfExecutor.extractJobIdSubmitted(
                "Job <2848143> is submitted to default queue <research-rh74>."))
        .isEqualTo("2848143");

    assertThat(AbstractLsfExecutor.extractJobIdSubmitted("Job <2848143> is submitted "))
        .isEqualTo("2848143");
  }

  @Test
  public void testExtractJobIdNotFound() {
    assertThat(AbstractLsfExecutor.extractJobIdNotFound("Job <345654> is not found.")).isTrue();
    assertThat(AbstractLsfExecutor.extractJobIdNotFound("Job <345654> is not found")).isTrue();
    assertThat(AbstractLsfExecutor.extractJobIdNotFound("Job <345654> is ")).isFalse();
  }

  @Test
  public void testExtractExitCode() {
    assertThat(AbstractLsfExecutor.extractExitCode("Exited with exit code 1")).isEqualTo("1");
    assertThat(AbstractLsfExecutor.extractExitCode("Exited with exit code 3.")).isEqualTo("3");
  }
}
