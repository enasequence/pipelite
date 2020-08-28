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
import pipelite.configuration.ProcessConfiguration;
import pipelite.executor.PermanentErrorTaskExecutor;
import pipelite.executor.SuccessTaskExecutor;
import pipelite.instance.ProcessInstance;
import pipelite.instance.ProcessInstanceBuilder;
import pipelite.resolver.DefaultExceptionResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = FullTestConfiguration.class,
    properties = {
      "pipelite.launcher.workers=5",
      "spring.autoconfigure.exclude="
          + "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
          + "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,"
          + "org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration"
    })
@ContextConfiguration(
    initializers = PipeliteInMemorySuccessTaskLauncherTest.TestContextInitializer.class)
@ActiveProfiles(value = {"test", "memory"})
public class PipeliteInMemoryFailingTaskLauncherTest {

  @Autowired private ProcessConfiguration processConfiguration;

  @Autowired private ObjectProvider<PipeliteLauncher> defaultPipeliteLauncherObjectProvider;

  private static final String PROCESS_NAME = UniqueStringGenerator.randomProcessName();
  private static final int PROCESS_CNT = 10;

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
                          new PermanentErrorTaskExecutor(),
                          new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          new SuccessTaskExecutor(),
                          new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          new SuccessTaskExecutor(),
                          new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(),
                          new SuccessTaskExecutor(),
                          new DefaultExceptionResolver())
                      .build());
            });

    PipeliteLauncher pipeliteLauncher = defaultPipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);

    pipeliteLauncher.setShutdownPolicy(PipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(pipeliteLauncher);

    assertThat(processFactory.getNewProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getReceivedProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getConfirmedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processFactory.getRejectedProcessInstances()).isEqualTo(0);

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
                      .task(UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new PermanentErrorTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .build());
            });

    PipeliteLauncher pipeliteLauncher = defaultPipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);

    pipeliteLauncher.setShutdownPolicy(PipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(pipeliteLauncher);

    assertThat(processFactory.getNewProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getReceivedProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getConfirmedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processFactory.getRejectedProcessInstances()).isEqualTo(0);

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
                      .task(UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new PermanentErrorTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .build());
            });

    PipeliteLauncher pipeliteLauncher = defaultPipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);

    pipeliteLauncher.setShutdownPolicy(PipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(pipeliteLauncher);

    assertThat(processFactory.getNewProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getReceivedProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getConfirmedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processFactory.getRejectedProcessInstances()).isEqualTo(0);

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
                      .task(UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new PermanentErrorTaskExecutor(), new DefaultExceptionResolver())
                      .build());
            });

    PipeliteLauncher pipeliteLauncher = defaultPipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);

    pipeliteLauncher.setShutdownPolicy(PipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(pipeliteLauncher);

    assertThat(processFactory.getNewProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getReceivedProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getConfirmedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processFactory.getRejectedProcessInstances()).isEqualTo(0);

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
                      .task(UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor(), new DefaultExceptionResolver())
                      .build());
            });

    PipeliteLauncher pipeliteLauncher = defaultPipeliteLauncherObjectProvider.getObject();

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);

    pipeliteLauncher.setShutdownPolicy(PipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(pipeliteLauncher);

    assertThat(processFactory.getNewProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getReceivedProcessInstances()).isEqualTo(0);
    assertThat(processFactory.getConfirmedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processFactory.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 4);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }
}
