package pipelite.process.launcher;

public interface ProcessLauncher extends Runnable {

    String getProcessId();

    void stop();
}
