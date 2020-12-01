package pipelite.launcher;

import pipelite.process.Process;

import java.util.function.BiFunction;

/** Creates a process launcher given a pipeline name and a process. */
public interface ProcessLauncherFactory extends BiFunction<String, Process, ProcessLauncher> {}
