package pipelite.launcher;

public interface ProcessLauncher extends AutoCloseable {

  boolean init(String processId);

  void execute();

  void stop();

  @Override
  void close();
}
