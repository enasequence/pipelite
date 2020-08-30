package pipelite.service;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteMonitor;

import static org.assertj.core.api.Assertions.assertThat;

class PipeliteInMemoryMonitorServiceTest {

  @Test
  public void testCrud() {

    PipeliteInMemoryMonitorService service = new PipeliteInMemoryMonitorService();

    String processId = UniqueStringGenerator.randomProcessId();
    String processName = UniqueStringGenerator.randomProcessName();
    String stageName = UniqueStringGenerator.randomTaskName();

    PipeliteMonitor monitor = new PipeliteMonitor();
    monitor.setProcessId(processId);
    monitor.setProcessName(processName);
    monitor.setStageName(stageName);

    service.saveMonitor(monitor);

    assertThat(service.getSavedMonitor(processName, processId, stageName).get()).isEqualTo(monitor);

    service.delete(monitor);

    assertThat(service.getSavedMonitor(processName, processId, stageName).isPresent()).isFalse();
  }
}
