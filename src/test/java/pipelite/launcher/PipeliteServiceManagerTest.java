package pipelite.launcher;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

public class PipeliteServiceManagerTest {

  private static class TestPipeliteService extends PipeliteService {

    private final AtomicLong schedulerCnt = new AtomicLong();
    private final AtomicLong runCnt = new AtomicLong();
    private final long maxRunCnt;

    public TestPipeliteService(long maxRunCnt) {
      this.maxRunCnt = maxRunCnt;
    }

    @Override
    public String serviceName() {
      return UniqueStringGenerator.randomLauncherName();
    }

    @Override
    protected Scheduler scheduler() {
      schedulerCnt.incrementAndGet();
      return Scheduler.newFixedDelaySchedule(Duration.ZERO, Duration.ofMillis(10));
    }

    @Override
    protected void runOneIteration() {
      if (runCnt.incrementAndGet() >= maxRunCnt) {
        stopAsync();
      }
    }

    public long getSchedulerCnt() {
      return schedulerCnt.get();
    }

    public long getRunCnt() {
      return runCnt.get();
    }
  };

  @Test
  public void run() {
    int maxRunCnt = 10;

    PipeliteServiceManager serviceManager = new PipeliteServiceManager();
    TestPipeliteService pipeliteService1 = new TestPipeliteService(maxRunCnt);
    TestPipeliteService pipeliteService2 = new TestPipeliteService(maxRunCnt);
    TestPipeliteService pipeliteService3 = new TestPipeliteService(maxRunCnt);

    serviceManager.add(pipeliteService1);
    serviceManager.add(pipeliteService2);
    serviceManager.add(pipeliteService3);

    serviceManager.run();

    assertThat(pipeliteService1.getSchedulerCnt()).isEqualTo(1);
    assertThat(pipeliteService2.getSchedulerCnt()).isEqualTo(1);
    assertThat(pipeliteService3.getSchedulerCnt()).isEqualTo(1);

    assertThat(pipeliteService1.getRunCnt()).isEqualTo(maxRunCnt);
    assertThat(pipeliteService2.getRunCnt()).isEqualTo(maxRunCnt);
    assertThat(pipeliteService3.getRunCnt()).isEqualTo(maxRunCnt);
  }
}
