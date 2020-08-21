package uk.ac.ebi.ena.sra.pipeline.launcher;

import pipelite.entity.PipeliteProcess;

public interface ProcessLauncherFactory {

  ProcessLauncher create(PipeliteProcess pipeliteProcess);
}
