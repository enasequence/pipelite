package pipelite.instance;

public interface ProcessInstanceFactory {
  ProcessInstance create(String processId);
}
