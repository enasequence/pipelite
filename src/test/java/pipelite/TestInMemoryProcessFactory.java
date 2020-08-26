package pipelite;

import com.google.common.util.concurrent.Monitor;
import pipelite.instance.ProcessInstance;
import pipelite.instance.ProcessInstanceFactory;

import java.util.*;

public class TestInMemoryProcessFactory implements ProcessInstanceFactory {

  private final Set<ProcessInstance> newProcessInstances = new HashSet<>();
  private final Map<String, ProcessInstance> receivedProcessInstances = new HashMap<>();
  private final Map<String, ProcessInstance> confirmedProcessInstances = new HashMap<>();
  private final Set<ProcessInstance> rejectedProcessInstances = new HashSet<>();

  public TestInMemoryProcessFactory(Collection<ProcessInstance> processInstances) {
    newProcessInstances.addAll(processInstances);
  }

  private boolean permanentRejection = false;

  private final Monitor monitor = new Monitor();

  @Override
  public ProcessInstance receive() {
    monitor.enter();
    try {
      if (newProcessInstances.isEmpty()) {
        return null;
      }
      ProcessInstance processInstance = newProcessInstances.iterator().next();
      receivedProcessInstances.put(processInstance.getProcessId(), processInstance);
      newProcessInstances.remove(processInstance);
      return processInstance;
    } finally {
      monitor.leave();
    }
  }

  @Override
  public void confirm(ProcessInstance processInstance) {
    monitor.enter();
    try {
      confirmedProcessInstances.put(
          processInstance.getProcessId(),
          receivedProcessInstances.remove(processInstance.getProcessId()));
    } finally {
      monitor.leave();
    }
  }

  @Override
  public void reject(ProcessInstance processInstance) {
    monitor.enter();
    try {
      if (permanentRejection) {
        rejectedProcessInstances.add(
            receivedProcessInstances.remove(processInstance.getProcessId()));
      } else {
        newProcessInstances.add(receivedProcessInstances.remove(processInstance.getProcessId()));
      }
    } finally {
      monitor.leave();
    }
  }

  @Override
  public ProcessInstance load(String processId) {
    monitor.enter();
    try {
      return confirmedProcessInstances.get(processId);
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

  public int getReceivedProcessInstances() {
    return receivedProcessInstances.size();
  }

  public int getConfirmedProcessInstances() {
    return confirmedProcessInstances.size();
  }

  public int getRejectedProcessInstances() {
    return rejectedProcessInstances.size();
  }
}
