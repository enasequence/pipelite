package pipelite.launcher;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import pipelite.FullTestConfiguration;
import pipelite.TestInMemoryProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfigurationEx;
import pipelite.executor.TaskExecutor;
import pipelite.executor.TaskExecutorFactory;
import pipelite.instance.ProcessInstance;
import pipelite.instance.ProcessInstanceBuilder;
import pipelite.task.TaskInfo;
import pipelite.task.result.TaskExecutionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = FullTestConfiguration.class,
    properties = {
      "pipelite.launcher.workers=5",
      "pipelite.process.executorFactoryName=pipelite.executor.InternalTaskExecutorFactory",
      "pipelite.task.resolver=pipelite.resolver.DefaultExceptionResolver"
    })
@ContextConfiguration(
    initializers = DefaultPipeliteDatabaseSuccessTaskLauncherTest.TestContextInitializer.class)
@ActiveProfiles(value = {"database", "database-oracle-test"})
public class DefaultPipeliteDatabaseFailingTaskLauncherTest {

  @Autowired private ProcessConfigurationEx processConfiguration;

  @Autowired private ObjectProvider<DefaultPipeliteLauncher> defaultPipeliteLauncherObjectProvider;

  private static final String PROCESS_NAME = UniqueStringGenerator.randomProcessName();
  private static final int PROCESS_CNT = 10;

  public static class SuccessTaskExecutionFactory implements TaskExecutorFactory {
    @Override
    public TaskExecutor createTaskExecutor() {
      return taskInstance -> TaskExecutionResult.success();
    }
  }

  public static class FailTaskExecutionFactory implements TaskExecutorFactory {
    @Override
    public TaskExecutor createTaskExecutor() {
      throw new RuntimeException();
    }
  }

  @Test
  public void testFirstTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<ProcessInstance> processInstances = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processInstances.add(
                  new ProcessInstanceBuilder(
                          PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task(UniqueStringGenerator.randomTaskName(), new FailTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .build());
            });

    DefaultPipeliteLauncher defaultPipeliteLauncher =
        defaultPipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);

    defaultPipeliteLauncher.setShutdownPolicy(
        DefaultPipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    defaultPipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(defaultPipeliteLauncher);

    assertThat(processFactory.getNewProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getReceivedProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getConfirmedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processFactory.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(defaultPipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(defaultPipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(defaultPipeliteLauncher.getTaskCompletedCount()).isEqualTo(0);
    assertThat(defaultPipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(defaultPipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }

  @Test
  public void testSecondTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<ProcessInstance> processInstances = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processInstances.add(
                  new ProcessInstanceBuilder(
                          PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new FailTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .build());
            });

    DefaultPipeliteLauncher defaultPipeliteLauncher =
        defaultPipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);

    defaultPipeliteLauncher.setShutdownPolicy(
        DefaultPipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    defaultPipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(defaultPipeliteLauncher);

    assertThat(processFactory.getNewProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getReceivedProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getConfirmedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processFactory.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(defaultPipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(defaultPipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(defaultPipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(defaultPipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(defaultPipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }

  @Test
  public void testThirdTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<ProcessInstance> processInstances = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processInstances.add(
                  new ProcessInstanceBuilder(
                          PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new FailTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .build());
            });

    DefaultPipeliteLauncher defaultPipeliteLauncher =
        defaultPipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);

    defaultPipeliteLauncher.setShutdownPolicy(
        DefaultPipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    defaultPipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(defaultPipeliteLauncher);

    assertThat(processFactory.getNewProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getReceivedProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getConfirmedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processFactory.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(defaultPipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(defaultPipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(defaultPipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 2);
    assertThat(defaultPipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(defaultPipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }

  @Test
  public void testFourthTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<ProcessInstance> processInstances = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processInstances.add(
                  new ProcessInstanceBuilder(
                          PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new FailTaskExecutionFactory())
                      .build());
            });

    DefaultPipeliteLauncher defaultPipeliteLauncher =
        defaultPipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);

    defaultPipeliteLauncher.setShutdownPolicy(
        DefaultPipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    defaultPipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(defaultPipeliteLauncher);

    assertThat(processFactory.getNewProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getReceivedProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getConfirmedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processFactory.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(defaultPipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(defaultPipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(defaultPipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 3);
    assertThat(defaultPipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(defaultPipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }

  @Test
  public void testNoTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<ProcessInstance> processInstances = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processInstances.add(
                  new ProcessInstanceBuilder(
                          PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutionFactory())
                      .build());
            });

    DefaultPipeliteLauncher defaultPipeliteLauncher =
        defaultPipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);

    defaultPipeliteLauncher.setShutdownPolicy(
        DefaultPipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    defaultPipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(defaultPipeliteLauncher);

    assertThat(processFactory.getNewProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getReceivedProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getConfirmedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processFactory.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(defaultPipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(defaultPipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(defaultPipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 4);
    assertThat(defaultPipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(defaultPipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }
}
