package pipelite.executor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import pipelite.EmptyTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.instance.TaskInstance;
import pipelite.task.result.TaskExecutionResult;
import uk.ac.ebi.ena.sra.pipeline.launcher.InternalTaskExecutor;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {
      "pipelite.task.memory=1",
      "pipelite.task.cores=1",
      "pipelite.task.queue=TEST",
      "pipelite.task.memoryTimeout=15",
      "pipelite.task.retries=3",
      "pipelite.task.tempdir=",
      "pipelite.task.env=TEST1,TEST2",
      "pipelite.task.resolver=pipelite.resolver.DefaultExceptionResolver",
      "pipelite.task.executorFactoryName="
    })
@EnableConfigurationProperties(
    value = {LauncherConfiguration.class, ProcessConfiguration.class, TaskConfiguration.class})
@ComponentScan(basePackageClasses = {TaskConfigurationEx.class})
public class InternalExecutorTest {

  @Autowired TaskConfigurationEx taskConfiguration;

  @Test
  public void test() {

    String processName = UniqueStringGenerator.randomProcessName();
    String processId = UniqueStringGenerator.randomProcessId();
    String taskName = UniqueStringGenerator.randomTaskName();

    AtomicInteger taskExecutionCount = new AtomicInteger();

    InternalTaskExecutor internalTaskExecutor = new InternalTaskExecutor();

    // Task specific configuration is not available when a task is being executed using internal
    // task executor.

    TaskExecutorFactory testTaskExecutorFactory =
        new TaskExecutorFactory() {
          @Override
          public TaskExecutor createTaskExecutor() {
            return taskInstance1 -> {
              taskExecutionCount.getAndIncrement();
              return TaskExecutionResult.success();
            };
          }
        };

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .taskExecutorFactory(testTaskExecutorFactory)
            .taskParameters(taskConfiguration)
            .build();

    TaskExecutionResult result = internalTaskExecutor.execute(taskInstance);
    assertThat(result).isEqualTo(TaskExecutionResult.success());
    assertThat(taskExecutionCount.get()).isEqualTo(1);
  }
}
