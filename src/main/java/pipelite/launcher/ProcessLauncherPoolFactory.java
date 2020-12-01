package pipelite.launcher;

import pipelite.launcher.lock.PipeliteLocker;

import java.util.function.Function;

/** Creates a process launcher pool given a locker. */
public interface ProcessLauncherPoolFactory extends Function<PipeliteLocker, ProcessLauncherPool> {}
