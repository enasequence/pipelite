package uk.ac.ebi.ena.sra.pipeline.launcher;

public interface ProcessLauncher extends Runnable {

    String getProcessId();

    void stop();
}
