package pipelite.process;

public interface ProcessFactory {
  ProcessInstance create(String processId);
}
