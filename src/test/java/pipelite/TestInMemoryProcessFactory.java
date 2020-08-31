package pipelite;

import pipelite.process.ProcessInstance;
import pipelite.process.ProcessFactory;

import java.util.*;

public class TestInMemoryProcessFactory implements ProcessFactory {

  private final Map<String, ProcessInstance> processInstances = new HashMap<>();

  public TestInMemoryProcessFactory(Collection<ProcessInstance> processInstances) {
    processInstances.stream()
        .forEach(
            processInstance ->
                this.processInstances.put(processInstance.getProcessId(), processInstance));
  }

  @Override
  public ProcessInstance create(String processId) {
    return processInstances.get(processId);
  }

  public int getProcessInstances() {
    return processInstances.size();
  }
}
