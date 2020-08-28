package pipelite.resolver;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.task.TaskExecutionResultType.PERMANENT_ERROR;
import static pipelite.task.TaskExecutionResultType.SUCCESS;

public class DefaultInternalTaskExecutorResolverTest {

  @Test
  public void test() {
    DefaultInternalTaskExecutorResolver resolver = new DefaultInternalTaskExecutorResolver();
    assertThat(resolver.resolve(new Throwable()).getResultType()).isEqualTo(PERMANENT_ERROR);
    assertThat(resolver.resolve(new Exception()).getResultType()).isEqualTo(PERMANENT_ERROR);
    assertThat(resolver.resolve(new RuntimeException()).getResultType()).isEqualTo(PERMANENT_ERROR);
    assertThat(resolver.resolve(null).getResultType()).isEqualTo(SUCCESS);
  }
}
