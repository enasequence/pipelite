package pipelite;

import com.google.common.util.concurrent.Monitor;
import pipelite.process.ProcessInstance;
import pipelite.process.ProcessSource;

import java.util.*;

public class TestInMemoryProcessSource implements ProcessSource {

  private final Set<ProcessInstance> newProcessInstances = new HashSet<>();
  private final Map<String, ProcessInstance> returnedProcessInstances = new HashMap<>();
  private final Map<String, ProcessInstance> acceptedProcessInstances = new HashMap<>();
  private final Set<ProcessInstance> rejectedProcessInstances = new HashSet<>();

  public TestInMemoryProcessSource(Collection<ProcessInstance> processInstances) {
    newProcessInstances.addAll(processInstances);
  }

  private boolean permanentRejection = false;

  private final Monitor monitor = new Monitor();

  @Override
  public NewProcess next() {
    monitor.enter();
    try {
      if (newProcessInstances.isEmpty()) {
        return null;
      }
      ProcessInstance processInstance = newProcessInstances.iterator().next();
      returnedProcessInstances.put(processInstance.getProcessId(), processInstance);
      newProcessInstances.remove(processInstance);
      return new NewProcess(processInstance.getProcessId(), 9);
    } finally {
      monitor.leave();
    }
  }

  @Override
  public void accept(String processId) {
    monitor.enter();
    try {
      acceptedProcessInstances.put(processId, returnedProcessInstances.remove(processId));
    } finally {
      monitor.leave();
    }
  }

  @Override
  public void reject(String processId) {
    monitor.enter();
    try {
      if (permanentRejection) {
        rejectedProcessInstances.add(returnedProcessInstances.remove(processId));
      } else {
        newProcessInstances.add(returnedProcessInstances.remove(processId));
      }
    } finally {
      monitor.leave();
    }
  }

  public boolean isPermanentRejection() {
    return permanentRejection;
  }

  public void setPermanentRejection(boolean permanentRejection) {
    this.permanentRejection = permanentRejection;
  }

  public int getNewProcessInstances() {
    return newProcessInstances.size();
  }

  public int getReturnedProcessInstances() {
    return returnedProcessInstances.size();
  }

  public int getAcceptedProcessInstances() {
    return acceptedProcessInstances.size();
  }

  public int getRejectedProcessInstances() {
    return rejectedProcessInstances.size();
  }
}
