package pipelite.launcher;

import org.junit.jupiter.api.Test;
import pipelite.configuration.LauncherConfiguration;
import pipelite.launcher.lock.PipeliteLocker;
import pipelite.process.ProcessState;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class ProcessLauncherServiceTest {

  public static final String LAUNCHER_NAME = "LAUNCHER1";

  @Test
  public void lifecycle() throws Exception {
    AtomicLong lockCallCnt = new AtomicLong();
    AtomicLong renewLockCallCnt = new AtomicLong();
    AtomicLong runCallCnt = new AtomicLong();
    AtomicLong unlockCallCnt = new AtomicLong();

    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    PipeliteLocker locker = mock(PipeliteLocker.class);

    doAnswer(
            a -> {
              lockCallCnt.incrementAndGet();
              return null;
            })
        .when(locker)
        .lock();
    doAnswer(
            a -> {
              renewLockCallCnt.incrementAndGet();
              return null;
            })
        .when(locker)
        .renewLock();
    doAnswer(
            a -> {
              unlockCallCnt.incrementAndGet();
              return null;
            })
        .when(locker)
        .unlock();

    ProcessLauncherService processLauncherService =
        spy(
            new ProcessLauncherService(
                launcherConfiguration,
                locker,
                LAUNCHER_NAME,
                (locker1) ->
                    new ProcessLauncherPool(
                        locker1,
                        (pipelineName1, process1) -> {
                          process1.getProcessEntity().endExecution(ProcessState.COMPLETED);
                          return mock(ProcessLauncher.class);
                        })) {
              @Override
              protected void run() {
                runCallCnt.incrementAndGet();
              }
            });
    doReturn(true).when(processLauncherService).isActive();

    assertThat(processLauncherService.getLauncherName()).isEqualTo(LAUNCHER_NAME);
    assertThat(processLauncherService.getPool()).isNull();
    assertThat(processLauncherService.getProcessLaunchers()).isEmpty();

    assertThat(lockCallCnt.get()).isZero();
    assertThat(renewLockCallCnt.get()).isZero();
    assertThat(runCallCnt.get()).isZero();
    assertThat(unlockCallCnt.get()).isZero();

    processLauncherService.startUp();

    assertThat(lockCallCnt.get()).isOne();
    assertThat(renewLockCallCnt.get()).isZero();
    assertThat(runCallCnt.get()).isZero();
    assertThat(unlockCallCnt.get()).isZero();

    processLauncherService.runOneIteration();

    assertThat(lockCallCnt.get()).isOne();
    assertThat(renewLockCallCnt.get()).isOne();
    assertThat(runCallCnt.get()).isOne();
    assertThat(unlockCallCnt.get()).isZero();

    processLauncherService.runOneIteration();

    assertThat(lockCallCnt.get()).isOne();
    assertThat(renewLockCallCnt.get()).isEqualTo(2);
    assertThat(runCallCnt.get()).isEqualTo(2);
    assertThat(unlockCallCnt.get()).isZero();

    processLauncherService.shutDown();

    assertThat(lockCallCnt.get()).isOne();
    assertThat(renewLockCallCnt.get()).isEqualTo(2);
    assertThat(runCallCnt.get()).isEqualTo(2);
    assertThat(unlockCallCnt.get()).isOne();
  }
}
