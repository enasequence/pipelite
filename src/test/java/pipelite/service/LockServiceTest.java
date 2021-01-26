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
package pipelite.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.entity.ServiceLockEntity;
import pipelite.time.Time;

@SpringBootTest(
    classes = PipeliteTestConfiguration.class,
    properties = {"pipelite.advanced.lockDuration=15s"})
public class LockServiceTest {

  private static Duration LOCK_DURATION = Duration.ofSeconds(15);

  @Autowired LockService service;

  @Autowired DataSource dataSource;

  @Test
  public void testParallelLockProcess() throws Exception {
    String serviceName1 = UniqueStringGenerator.randomServiceName();
    ServiceLockEntity serviceLock1 = LockService.lockService(service, serviceName1);

    String pipelineName = UniqueStringGenerator.randomPipelineName();
    ExecutorService executorService = Executors.newFixedThreadPool(500);
    for (int i = 0; i < 500; ++i) {
      executorService.submit(
          () -> {
            assertThat(
                    LockService.lockProcess(
                        service,
                        serviceLock1,
                        pipelineName,
                        UniqueStringGenerator.randomProcessId()))
                .isEqualTo(true);
          });
    }
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
  }

  @Test
  public void testServiceLocks() {
    String serviceName1 = UniqueStringGenerator.randomServiceName();
    String serviceName2 = UniqueStringGenerator.randomServiceName();

    service.getServiceLocksByServiceName(serviceName1).forEach(s -> service.unlockService(s));
    service.getServiceLocksByServiceName(serviceName2).forEach(s -> service.unlockService(s));

    ServiceLockEntity serviceLock1 = LockService.lockService(service, serviceName1);
    ServiceLockEntity serviceLock2 = LockService.lockService(service, serviceName2);

    ZonedDateTime expiry1 = serviceLock1.getExpiry();
    ZonedDateTime expiry2 = serviceLock2.getExpiry();

    assertThat(serviceLock1.getServiceId()).isGreaterThan(0);
    assertThat(serviceLock1.getServiceName()).isEqualTo(serviceName1);
    assertThat(serviceLock1.getExpiry())
        .isAfterOrEqualTo(ZonedDateTime.now().plus(service.getLockDuration()).minus(LOCK_DURATION));

    assertThat(serviceLock2.getServiceId()).isGreaterThan(0);
    assertThat(serviceLock2.getServiceName()).isEqualTo(serviceName2);
    assertThat(serviceLock2.getExpiry())
        .isAfterOrEqualTo(ZonedDateTime.now().plus(service.getLockDuration()).minus(LOCK_DURATION));

    assertThat(serviceLock1.getServiceId()).isLessThan(serviceLock2.getServiceId());

    assertThat(service.getServiceLocksByServiceName(serviceName1).size()).isOne();
    assertThat(service.getServiceLocksByServiceName(serviceName2).size()).isOne();
    assertThat(service.getServiceLocksByServiceName(serviceName1).get(0).getServiceId())
        .isEqualTo(serviceLock1.getServiceId());
    assertThat(service.getServiceLocksByServiceName(serviceName2).get(0).getServiceId())
        .isEqualTo(serviceLock2.getServiceId());

    assertTrue(service.relockService(serviceLock1));
    assertTrue(service.relockService(serviceLock2));

    assertThat(service.getServiceLocksByServiceName(serviceName1).size()).isOne();
    assertThat(service.getServiceLocksByServiceName(serviceName2).size()).isOne();
    assertThat(service.getServiceLocksByServiceName(serviceName1).get(0).getServiceId())
        .isEqualTo(serviceLock1.getServiceId());
    assertThat(service.getServiceLocksByServiceName(serviceName2).get(0).getServiceId())
        .isEqualTo(serviceLock2.getServiceId());
    assertThat(service.getServiceLocksByServiceName(serviceName1).get(0).getExpiry())
        .isAfterOrEqualTo(expiry1);
    assertThat(service.getServiceLocksByServiceName(serviceName2).get(0).getExpiry())
        .isAfterOrEqualTo(expiry2);

    service.unlockService(serviceLock1);
    service.unlockService(serviceLock2);
    assertThat(service.getServiceLocksByServiceName(serviceName1).size()).isZero();
    assertThat(service.getServiceLocksByServiceName(serviceName2).size()).isZero();
  }

  @Test
  public void testProcessLocks() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String serviceName1 = UniqueStringGenerator.randomServiceName();
    String serviceName2 = UniqueStringGenerator.randomServiceName();

    service.getServiceLocksByServiceName(serviceName1).forEach(s -> service.unlockService(s));
    service.getServiceLocksByServiceName(serviceName2).forEach(s -> service.unlockService(s));

    ServiceLockEntity serviceLock1 = LockService.lockService(service, serviceName1);
    ServiceLockEntity serviceLock2 = LockService.lockService(service, serviceName2);

    assertTrue(LockService.lockProcess(service, serviceLock1, pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "1"));

    assertTrue(LockService.lockProcess(service, serviceLock1, pipelineName, "2"));
    assertTrue(service.isProcessLocked(pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "2"));

    assertTrue(service.unlockProcess(serviceLock1, pipelineName, "1"));
    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "2"));

    assertTrue(LockService.lockProcess(service, serviceLock2, pipelineName, "3"));
    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "2"));
    assertTrue(service.isProcessLocked(pipelineName, "3"));

    assertTrue(LockService.lockProcess(service, serviceLock2, pipelineName, "4"));
    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "2"));
    assertTrue(service.isProcessLocked(pipelineName, "3"));
    assertTrue(service.isProcessLocked(pipelineName, "4"));

    assertTrue(service.unlockProcess(serviceLock2, pipelineName, "4"));
    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "2"));
    assertTrue(service.isProcessLocked(pipelineName, "3"));
    assertFalse(service.isProcessLocked(pipelineName, "4"));

    service.unlockProcesses(serviceLock1);

    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertFalse(service.isProcessLocked(pipelineName, "2"));
    assertTrue(service.isProcessLocked(pipelineName, "3"));
    assertFalse(service.isProcessLocked(pipelineName, "4"));

    service.unlockProcesses(serviceLock2);

    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertFalse(service.isProcessLocked(pipelineName, "2"));
    assertFalse(service.isProcessLocked(pipelineName, "3"));
    assertFalse(service.isProcessLocked(pipelineName, "4"));
  }

  // Test fails if the lock is not created and checked for the first time within the lock duration.
  @Test
  public void testRemoveExpiredServiceLock() {
    String serviceName1 = UniqueStringGenerator.randomServiceName();
    String serviceName2 = UniqueStringGenerator.randomServiceName();

    service.getServiceLocksByServiceName(serviceName1).forEach(s -> service.unlockService(s));
    service.getServiceLocksByServiceName(serviceName2).forEach(s -> service.unlockService(s));

    assertThat(LockService.lockService(service, serviceName1)).isNotNull();
    assertThat(service.isServiceLocked(serviceName1)).isTrue();

    // Expired lock will not be removed.
    assertThat(LockService.lockService(service, serviceName1)).isNull();
    assertThat(service.isServiceLocked(serviceName1)).isTrue();

    Time.wait(service.getLockDuration());

    // Expired lock will be removed.
    assertThat(LockService.lockService(service, serviceName1)).isNotNull();
    assertThat(service.isServiceLocked(serviceName1)).isTrue();
  }

  // Test fails if the lock is not created and checked for the first time within the lock duration.
  @Test
  public void testRemoveExpiredProcessLock() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String serviceName1 = UniqueStringGenerator.randomServiceName();
    String serviceName2 = UniqueStringGenerator.randomServiceName();

    service.getServiceLocksByServiceName(serviceName1).forEach(s -> service.unlockService(s));
    service.getServiceLocksByServiceName(serviceName2).forEach(s -> service.unlockService(s));

    ServiceLockEntity serviceLock1 = LockService.lockService(service, serviceName1);
    ServiceLockEntity serviceLock2 = LockService.lockService(service, serviceName2);

    assertThat(service.isServiceLocked(serviceName1)).isTrue();
    assertThat(service.isServiceLocked(serviceName2)).isTrue();

    String processId = "1";

    assertThat(service.isProcessLocked(pipelineName, processId)).isFalse();
    assertThat(LockService.lockProcess(service, serviceLock1, pipelineName, processId)).isTrue();
    assertThat(LockService.lockProcess(service, serviceLock2, pipelineName, processId)).isFalse();
    assertThat(service.isProcessLocked(pipelineName, processId)).isTrue();

    // Expired lock will not be removed.
    assertThat(LockService.lockProcess(service, serviceLock2, pipelineName, processId)).isFalse();
    assertThat(service.isProcessLocked(pipelineName, processId)).isTrue();

    Time.wait(service.getLockDuration());

    // Expired lock will be removed.
    service.relockService(serviceLock2);
    assertThat(LockService.lockProcess(service, serviceLock2, pipelineName, processId)).isTrue();
    assertThat(service.isProcessLocked(pipelineName, processId)).isTrue();
  }
}
