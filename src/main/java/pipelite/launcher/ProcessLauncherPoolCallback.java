package pipelite.launcher;

import java.util.function.BiConsumer;
import pipelite.process.Process;

/** Callback at the end of process execution from the process launcher pool. */
public interface ProcessLauncherPoolCallback
    extends BiConsumer<Process, ProcessLauncherPool.Result> {}
