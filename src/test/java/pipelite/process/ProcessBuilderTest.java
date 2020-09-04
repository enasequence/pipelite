package pipelite.process;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.executor.SuccessTaskExecutor;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessBuilderTest {

  private static final String PROCESS_NAME = UniqueStringGenerator.randomProcessName();
  private static final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  @Test
  public void test() {

    IntStream.range(0, 10)
        .forEach(
            i -> {
              ProcessInstance processInstance =
                  new ProcessBuilder(PROCESS_NAME, PROCESS_ID, 9)
                      .task(UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .build();

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
