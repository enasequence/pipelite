package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.context.annotation.ComponentScan;
import pipelite.EmptyTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.instance.ProcessInstance;
import pipelite.instance.ProcessInstanceFactory;
import pipelite.instance.TaskInstance;
import pipelite.task.Task;
import pipelite.task.TaskFactory;
import pipelite.task.TaskInfo;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {
      "pipelite.process.processFactoryName=pipelite.configuration.ProcessConfigurationProcessFactoryTest$TestProcessFactory"
    })
@EnableConfigurationProperties(
    value = {LauncherConfiguration.class, ProcessConfiguration.class, TaskConfiguration.class})
@ComponentScan(basePackageClasses = {ProcessConfigurationEx.class})
public class ProcessConfigurationProcessFactoryTest {

  @Autowired ProcessConfigurationEx processConfiguration;

  private static final String PROCESS_NAME = UniqueStringGenerator.randomProcessName();
  private static final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  public static class TestTaskFactory implements TaskFactory {
    @Override
    public Task createTask(TaskInfo taskInfo) {
      return instance -> {};
    }
  }

  public static class TestProcessFactory implements ProcessInstanceFactory {

    @Override
    public ProcessInstance receive() {
      TaskFactory taskFactory = new TestTaskFactory();
      TaskConfigurationEx taskConfiguration = new TaskConfigurationEx(new TaskConfiguration());

      TaskInstance taskInstance1 =
          TaskInstance.builder()
              .processName(PROCESS_NAME)
              .processId(PROCESS_ID)
              .taskName(UniqueStringGenerator.randomTaskName())
              .taskFactory(taskFactory)
              .taskParameters(taskConfiguration)
              .build();

      TaskInstance taskInstance2 =
          TaskInstance.builder()
              .processName(PROCESS_NAME)
              .processId(PROCESS_ID)
              .taskName(UniqueStringGenerator.randomTaskName())
              .taskFactory(taskFactory)
              .dependsOn(taskInstance1)
              .taskParameters(taskConfiguration)
              .build();

      return ProcessInstance.builder()
          .processName(PROCESS_NAME)
          .processId(PROCESS_ID)
          .priority(9)
          .tasks(Arrays.asList(taskInstance1, taskInstance2))
          .build();
    }

    @Override
    public void confirm(ProcessInstance processInstance) {}

    @Override
    public void reject(ProcessInstance processInstance) {}

    @Override
    public ProcessInstance load(String processId) {
      return null;
    }
  }

  @Test
  public void test() {
    IntStream.range(0, 10)
        .forEach(
            i -> {
              ProcessInstance processInstance = processConfiguration.getProcessFactory().receive();
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
