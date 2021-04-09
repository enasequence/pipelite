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
package pipelite.manager;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.UniqueStringGenerator;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=RegisteredServiceManagerTest"
    })
@ActiveProfiles("test")
public class RegisteredServiceManagerTest {

  @Autowired RegisteredServiceManager registeredServiceManager;

  private static class TestRegisteredService extends RegisteredService {

    private final AtomicLong schedulerCnt = new AtomicLong();
    private final AtomicLong runCnt = new AtomicLong();
    private final long maxRunCnt;

    public TestRegisteredService(long maxRunCnt) {
      this.maxRunCnt = maxRunCnt;
    }

    @Override
    public String getLauncherName() {
      return UniqueStringGenerator.randomLauncherName(RegisteredServiceManagerTest.class);
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
    public void terminateProcesses() {}

    public long getSchedulerCnt() {
      return schedulerCnt.get();
    }

    public long getRunCnt() {
      return runCnt.get();
    }
  }

  @Test
  public void test() {
    final int maxRunCnt = 10;

    TestRegisteredService registeredService1 = new TestRegisteredService(maxRunCnt);
    TestRegisteredService registeredService2 = new TestRegisteredService(maxRunCnt);
    TestRegisteredService registeredService3 = new TestRegisteredService(maxRunCnt);

    registeredServiceManager.addService(registeredService1);
    registeredServiceManager.addService(registeredService2);
    registeredServiceManager.addService(registeredService3);
    registeredServiceManager.initServices();
    registeredServiceManager.start();
    registeredServiceManager.awaitStopped();

    assertThat(registeredService1.getSchedulerCnt()).isEqualTo(1);
    assertThat(registeredService2.getSchedulerCnt()).isEqualTo(1);
    assertThat(registeredService3.getSchedulerCnt()).isEqualTo(1);

    assertThat(registeredService1.getRunCnt()).isEqualTo(maxRunCnt);
    assertThat(registeredService2.getRunCnt()).isEqualTo(maxRunCnt);
    assertThat(registeredService3.getRunCnt()).isEqualTo(maxRunCnt);
  }
}
