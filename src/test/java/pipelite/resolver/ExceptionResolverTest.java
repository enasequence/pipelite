package pipelite.resolver;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.task.TaskExecutionResultType.PERMANENT_ERROR;

public class ExceptionResolverTest {

  @Test
  public void test() {
    ExceptionResolver resolver = ResultResolver.DEFAULT_EXCEPTION_RESOLVER;
    assertThat(resolver.resolve(new Throwable()).getResultType()).isEqualTo(PERMANENT_ERROR);
    assertThat(resolver.resolve(new Exception()).getResultType()).isEqualTo(PERMANENT_ERROR);
    assertThat(resolver.resolve(new RuntimeException()).getResultType()).isEqualTo(PERMANENT_ERROR);
    assertThat(resolver.resolve(null).getResultType()).isEqualTo(PERMANENT_ERROR);
  }
}
