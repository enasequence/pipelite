package pipelite.resolver;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.task.TaskExecutionResultType.ERROR;

public class ExceptionResolverTest {

  @Test
  public void test() {
    ExceptionResolver resolver = ResultResolver.DEFAULT_EXCEPTION_RESOLVER;
    assertThat(resolver.resolve(new Throwable()).getResultType()).isEqualTo(ERROR);
    assertThat(resolver.resolve(new Exception()).getResultType()).isEqualTo(ERROR);
    assertThat(resolver.resolve(new RuntimeException()).getResultType()).isEqualTo(ERROR);
    assertThat(resolver.resolve(null).getResultType()).isEqualTo(ERROR);
  }
}
