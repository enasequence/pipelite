package uk.ac.ebi.ena.sra.pipeline.launcher.iface;

public interface 
StageTask
{
	public void    init( Object id, boolean do_commit ) throws Throwable;
	public void    execute() throws Throwable;
	public void    unwind();
}
