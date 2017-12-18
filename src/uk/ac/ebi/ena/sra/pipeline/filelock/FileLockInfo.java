package uk.ac.ebi.ena.sra.pipeline.filelock;

public class 
FileLockInfo 
{
	final int    port;
	final String machine;
	final String machine_id;
	final String path;
	
	
	FileLockInfo( String path, String machine_id, String machine, int port )
	{
		this.path = path;
		this.machine_id = machine_id;
		this.machine = machine;
		this.port = port;
	}
	
	
	public String
	toString()
	{
		return String.format( "%s [id: %s, host: %s, port: %d]", path, machine_id, machine, port );
	}
}
