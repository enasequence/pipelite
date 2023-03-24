/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
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
import org.springframework.test.annotation.DirtiesContext;
import pipelite.entity.ServiceLockEntity;
import pipelite.test.PipeliteTestIdCreator;
import pipelite.test.configuration.PipeliteTestConfigWithServices;
import pipelite.time.Time;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=LockServiceTest",
      "pipelite.advanced.lockDuration=15s"
    })
@DirtiesContext
public class LockServiceTest {

  private static final Duration LOCK_DURATION = Duration.ofSeconds(15);

  @Autowired LockService lockService;
  @Autowired DataSource dataSource;

  @Test
  public void testParallelLockProcess() throws Exception {
    String serviceName1 = PipeliteTestIdCreator.serviceName();

    ServiceLockEntity serviceLock1 = LockService.lockService(lockService, serviceName1);

    String pipelineName = PipeliteTestIdCreator.pipelineName();
    ExecutorService executorService = Executors.newFixedThreadPool(500);
    for (int i = 0; i < 500; ++i) {
      executorService.submit(
          () -> {
            assertThat(
                    LockService.lockProcess(
                        lockService, serviceLock1, pipelineName, PipeliteTestIdCreator.processId()))
                .isEqualTo(true);
          });
    }
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
  }

  @Test
  public void testServiceLocks() {
    String serviceName1 = PipeliteTestIdCreator.serviceName();
    String serviceName2 = PipeliteTestIdCreator.serviceName();

    lockService.unlockService(serviceName1);
    lockService.unlockService(serviceName2);

    ServiceLockEntity serviceLock1 = LockService.lockService(lockService, serviceName1);
    ServiceLockEntity serviceLock2 = LockService.lockService(lockService, serviceName2);

    assertThat(serviceLock1.getServiceName()).isEqualTo(serviceName1);
    assertThat(serviceLock1.getExpiry())
        .isAfterOrEqualTo(
            ZonedDateTime.now().plus(lockService.getLockDuration()).minus(LOCK_DURATION));

    assertThat(serviceLock2.getServiceName()).isEqualTo(serviceName2);
    assertThat(serviceLock2.getExpiry())
        .isAfterOrEqualTo(
            ZonedDateTime.now().plus(lockService.getLockDuration()).minus(LOCK_DURATION));

    assertThat(lockService.isServiceLocked(serviceName1)).isTrue();
    assertThat(lockService.isServiceLocked(serviceName2)).isTrue();

    assertTrue(lockService.relockService(serviceLock1));
    assertTrue(lockService.relockService(serviceLock2));

    assertThat(lockService.isServiceLocked(serviceName1)).isTrue();
    assertThat(lockService.isServiceLocked(serviceName2)).isTrue();

    lockService.unlockService(serviceName1);
    lockService.unlockService(serviceName2);

    assertThat(lockService.isServiceLocked(serviceName1)).isFalse();
    assertThat(lockService.isServiceLocked(serviceName2)).isFalse();
  }

  @Test
  public void testProcessLocks() {
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String serviceName1 = PipeliteTestIdCreator.serviceName();
    String serviceName2 = PipeliteTestIdCreator.serviceName();

    lockService.unlockService(serviceName1);
    lockService.unlockService(serviceName2);

    ServiceLockEntity serviceLock1 = LockService.lockService(lockService, serviceName1);
    ServiceLockEntity serviceLock2 = LockService.lockService(lockService, serviceName2);

    assertTrue(LockService.lockProcess(lockService, serviceLock1, pipelineName, "1"));
    assertTrue(lockService.isProcessLocked(pipelineName, "1"));

    assertTrue(LockService.lockProcess(lockService, serviceLock1, pipelineName, "2"));
    assertTrue(lockService.isProcessLocked(pipelineName, "1"));
    assertTrue(lockService.isProcessLocked(pipelineName, "2"));

    assertTrue(lockService.unlockProcess(serviceLock1, pipelineName, "1"));
    assertFalse(lockService.isProcessLocked(pipelineName, "1"));
    assertTrue(lockService.isProcessLocked(pipelineName, "2"));

    assertTrue(LockService.lockProcess(lockService, serviceLock2, pipelineName, "3"));
    assertFalse(lockService.isProcessLocked(pipelineName, "1"));
    assertTrue(lockService.isProcessLocked(pipelineName, "2"));
    assertTrue(lockService.isProcessLocked(pipelineName, "3"));

    assertTrue(LockService.lockProcess(lockService, serviceLock2, pipelineName, "4"));
    assertFalse(lockService.isProcessLocked(pipelineName, "1"));
    assertTrue(lockService.isProcessLocked(pipelineName, "2"));
    assertTrue(lockService.isProcessLocked(pipelineName, "3"));
    assertTrue(lockService.isProcessLocked(pipelineName, "4"));

    assertTrue(lockService.unlockProcess(serviceLock2, pipelineName, "4"));
    assertFalse(lockService.isProcessLocked(pipelineName, "1"));
    assertTrue(lockService.isProcessLocked(pipelineName, "2"));
    assertTrue(lockService.isProcessLocked(pipelineName, "3"));
    assertFalse(lockService.isProcessLocked(pipelineName, "4"));

    lockService.unlockProcesses(serviceName1);

    assertFalse(lockService.isProcessLocked(pipelineName, "1"));
    assertFalse(lockService.isProcessLocked(pipelineName, "2"));
    assertTrue(lockService.isProcessLocked(pipelineName, "3"));
    assertFalse(lockService.isProcessLocked(pipelineName, "4"));

    lockService.unlockProcesses(serviceName2);

    assertFalse(lockService.isProcessLocked(pipelineName, "1"));
    assertFalse(lockService.isProcessLocked(pipelineName, "2"));
    assertFalse(lockService.isProcessLocked(pipelineName, "3"));
    assertFalse(lockService.isProcessLocked(pipelineName, "4"));
  }

  // Test fails if the lock is not created and checked for the first time within the lock duration.
  @Test
  public void testRemoveExpiredServiceLock() {
    String serviceName1 = PipeliteTestIdCreator.serviceName();

    lockService.unlockService(serviceName1);

    ServiceLockEntity serviceLockEntity1 = LockService.lockService(lockService, serviceName1);
    assertThat(serviceLockEntity1).isNotNull();
    assertThat(lockService.isServiceLocked(serviceName1)).isTrue();

    // Expired lock will not be removed.
    ServiceLockEntity serviceLockEntity2 = LockService.lockService(lockService, serviceName1);
    assertThat(serviceLockEntity2).isNull();
    assertThat(lockService.isServiceLocked(serviceName1)).isTrue();

    Time.wait(lockService.getLockDuration());

    // Expired lock will be removed.
    ServiceLockEntity serviceLockEntity3 = LockService.lockService(lockService, serviceName1);
    assertThat(serviceLockEntity3).isNotNull();
    assertThat(lockService.isServiceLocked(serviceName1)).isTrue();
    assertThat(serviceLockEntity3.getExpiry()).isAfter(serviceLockEntity1.getExpiry());
  }

  @Test
  public void testRemoveExpiredProcessLock() {
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String serviceName1 = PipeliteTestIdCreator.serviceName();
    String serviceName2 = PipeliteTestIdCreator.serviceName();

    lockService.unlockService(serviceName1);
    lockService.unlockService(serviceName2);

    assertThat(lockService.isServiceLocked(serviceName1)).isFalse();
    assertThat(lockService.isServiceLocked(serviceName2)).isFalse();

    ServiceLockEntity serviceLock1 = LockService.lockService(lockService, serviceName1);
    ServiceLockEntity serviceLock2 = LockService.lockService(lockService, serviceName2);

    assertThat(lockService.isServiceLocked(serviceName1)).isTrue();
    assertThat(lockService.isServiceLocked(serviceName2)).isTrue();

    String processId = "1";

    assertThat(lockService.isProcessLocked(pipelineName, processId)).isFalse();
    assertThat(LockService.lockProcess(lockService, serviceLock1, pipelineName, processId))
        .isTrue();
    assertThat(LockService.lockProcess(lockService, serviceLock2, pipelineName, processId))
        .isFalse();
    assertThat(lockService.isProcessLocked(pipelineName, processId)).isTrue();

    // Expired lock will not be removed.
    assertThat(LockService.lockProcess(lockService, serviceLock2, pipelineName, processId))
        .isFalse();
    assertThat(lockService.isProcessLocked(pipelineName, processId)).isTrue();

    Time.wait(lockService.getLockDuration());

    // Renew lock.
    lockService.relockService(serviceLock2);

    // Expired lock will be removed.
    assertThat(LockService.lockProcess(lockService, serviceLock2, pipelineName, processId))
        .isTrue();
    assertThat(lockService.isProcessLocked(pipelineName, processId)).isTrue();
  }
}
