/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.launcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;

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

    @Override
    public void terminate() {}

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

    serviceManager.addService(pipeliteService1);
    serviceManager.addService(pipeliteService2);
    serviceManager.addService(pipeliteService3);

    serviceManager.runSync();

    assertThat(pipeliteService1.getSchedulerCnt()).isEqualTo(1);
    assertThat(pipeliteService2.getSchedulerCnt()).isEqualTo(1);
    assertThat(pipeliteService3.getSchedulerCnt()).isEqualTo(1);

    assertThat(pipeliteService1.getRunCnt()).isEqualTo(maxRunCnt);
    assertThat(pipeliteService2.getRunCnt()).isEqualTo(maxRunCnt);
    assertThat(pipeliteService3.getRunCnt()).isEqualTo(maxRunCnt);
  }
}
