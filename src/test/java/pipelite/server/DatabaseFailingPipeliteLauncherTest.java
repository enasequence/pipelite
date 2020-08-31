package pipelite.server;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import pipelite.FullTestConfiguration;
import pipelite.TestInMemoryProcessFactory;
import pipelite.TestInMemoryProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfiguration;
import pipelite.executor.TaskExecutor;
import pipelite.instance.ProcessInstance;
import pipelite.instance.ProcessInstanceBuilder;
import pipelite.resolver.ResultResolver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = FullTestConfiguration.class,
    properties = {
      "pipelite.launcher.workers=5",
      "pipelite.task.resolver=pipelite.resolver.DefaultExceptionResolver"
    })
@ContextConfiguration(
    initializers = DatabaseSuccessPipeliteLauncherTest.TestContextInitializer.class)
@ActiveProfiles(value = {"database", "database-oracle-test"})
public class DatabaseFailingPipeliteLauncherTest {

  @Autowired private ProcessConfiguration processConfiguration;

  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;

  private static final String PROCESS_NAME = UniqueStringGenerator.randomProcessName();
  private static final int PROCESS_CNT = 10;
  private static final Duration SCHEDULER_DELAY = Duration.ofMillis(10);

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
                      .task(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.PERMANENT_ERROR_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .build());
            });

    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processInstances);
    processConfiguration.setProcessFactory(processFactory);
    processConfiguration.setProcessSource(processSource);

    pipeliteLauncher.setShutdownPolicy(ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelay(SCHEDULER_DELAY);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcessInstances()).isEqualTo(0);
    assertThat(processSource.getReturnedProcessInstances()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
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
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.PERMANENT_ERROR_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .build());
            });

    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processInstances);
    processConfiguration.setProcessFactory(processFactory);
    processConfiguration.setProcessSource(processSource);

    pipeliteLauncher.setShutdownPolicy(ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelay(SCHEDULER_DELAY);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcessInstances()).isEqualTo(0);
    assertThat(processSource.getReturnedProcessInstances()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
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
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.PERMANENT_ERROR_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .build());
            });

    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processInstances);
    processConfiguration.setProcessFactory(processFactory);
    processConfiguration.setProcessSource(processSource);

    pipeliteLauncher.setShutdownPolicy(ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelay(SCHEDULER_DELAY);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcessInstances()).isEqualTo(0);
    assertThat(processSource.getReturnedProcessInstances()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 2);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
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
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.PERMANENT_ERROR_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .build());
            });

    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processInstances);
    processConfiguration.setProcessFactory(processFactory);
    processConfiguration.setProcessSource(processSource);

    pipeliteLauncher.setShutdownPolicy(ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelay(SCHEDULER_DELAY);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcessInstances()).isEqualTo(0);
    assertThat(processSource.getReturnedProcessInstances()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 3);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
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
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          TaskExecutor.SUCCESS_EXECUTOR,
                          ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
                      .build());
            });

    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processInstances);
    processConfiguration.setProcessFactory(processFactory);
    processConfiguration.setProcessSource(processSource);

    pipeliteLauncher.setShutdownPolicy(ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelay(SCHEDULER_DELAY);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcessInstances()).isEqualTo(0);
    assertThat(processSource.getReturnedProcessInstances()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 4);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }
}
