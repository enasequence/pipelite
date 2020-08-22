package pipelite.process.launcher;

import pipelite.entity.PipeliteProcess;
import pipelite.process.launcher.ProcessLauncher;

public interface ProcessLauncherFactory {

  ProcessLauncher create(PipeliteProcess pipeliteProcess);
}
