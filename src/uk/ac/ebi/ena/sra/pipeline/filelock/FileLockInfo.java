package uk.ac.ebi.ena.sra.pipeline.filelock;

public class 
FileLockInfo 
{
	final int    port;
	final String machine;
	final int    pid;
	final String path;
	
	FileLockInfo( String path, String machine_id, int port )
	{
		this.path = path;
		this.pid = Integer.parseInt( machine_id.split( "@" )[ 0 ] );
		this.machine = machine_id.split( "@" )[ 1 ];
		this.port = port;
	}
	
	
	FileLockInfo( String path, int pid, String machine, int port )
	{
		this.path = path;
		this.pid = pid;
		this.machine = machine;
		this.port = port;
	}
	
	
	public String
	toString()
	{
		return String.format( "%s [pid: %s, host: %s, port: %d]", path, pid, machine, port );
	}
}
