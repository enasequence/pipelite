package pipelite.process.launcher;

import java.util.concurrent.Callable;

public interface ProcessLauncher extends Callable<Boolean> {

    String getProcessId();

    void stop();
}
