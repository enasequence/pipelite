package pipelite.instance;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.executor.TaskExecutor;
import pipelite.resolver.ResultResolver;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessInstanceBuilderTest {

  private static final String PROCESS_NAME = UniqueStringGenerator.randomProcessName();
  private static final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  public static class TestProcessSource implements ProcessInstanceSource {

    @Override
    public ProcessInstance next() {
      return new ProcessInstanceBuilder(PROCESS_NAME, PROCESS_ID, 9)
          .task(
              UniqueStringGenerator.randomTaskName(),
              TaskExecutor.SUCCESS_EXECUTOR,
              ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
          .taskDependsOnPrevious(
              UniqueStringGenerator.randomTaskName(),
              TaskExecutor.SUCCESS_EXECUTOR,
              ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
          .build();
    }

    @Override
    public void accept(ProcessInstance processInstance) {}

    @Override
    public void reject(ProcessInstance processInstance) {}
  }

  @Test
  public void test() {
    TestProcessSource testProcessSource = new TestProcessSource();

    IntStream.range(0, 10)
        .forEach(
            i -> {
              ProcessInstance processInstance = testProcessSource.next();
              assertThat(processInstance).isNotNull();
              assertThat(processInstance.getProcessName()).isEqualTo(PROCESS_NAME);
              assertThat(processInstance.getProcessId()).isEqualTo(PROCESS_ID);
              assertThat(processInstance.getPriority()).isEqualTo(9);
              assertThat(processInstance.getTasks().get(0).getProcessName())
                  .isEqualTo(PROCESS_NAME);
              assertThat(processInstance.getTasks().get(1).getProcessName())
                  .isEqualTo(PROCESS_NAME);
              assertThat(processInstance.getTasks().get(0).getProcessId()).isEqualTo(PROCESS_ID);
              assertThat(processInstance.getTasks().get(1).getProcessId()).isEqualTo(PROCESS_ID);
              assertThat(processInstance.getTasks()).hasSize(2);
            });
  }
}
