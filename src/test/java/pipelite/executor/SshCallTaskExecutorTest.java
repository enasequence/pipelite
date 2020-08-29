package pipelite.executor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.EmptyTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.TestConfiguration;
import pipelite.instance.TaskInstance;
import pipelite.instance.TaskParameters;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = EmptyTestConfiguration.class)
@EnableConfigurationProperties(value = {TestConfiguration.class})
@ActiveProfiles("test")
public class SshCallTaskExecutorTest {

  @Autowired TestConfiguration testConfiguration;

  @Test
  public void test() {

    SshCallTaskExecutor taskExecutor =
        SshCallTaskExecutor.builder().executable("echo").arguments(Arrays.asList("test")).build();

    String processName = UniqueStringGenerator.randomProcessName();
    String processId = UniqueStringGenerator.randomProcessId();
    String taskName = UniqueStringGenerator.randomTaskName();

    TaskParameters taskParameters = TaskParameters.builder().build();
    taskParameters.setHost(testConfiguration.getSsh().getHost());

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            // Executor is not required by SshCallTaskExecutor
            // .executor()
            .resolver(ResultResolver.DEFAULT_EXIT_CODE_RESOLVER)
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = taskExecutor.execute(taskInstance);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .isEqualTo("echo test");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE))
        .isEqualTo("0");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT))
        .isEqualTo("test\n");
  }
}
