package uk.ac.ebi.ena.sra.pipeline.filelock;

public class 
FileLockInfo 
{
	final public int    port;
	final public String machine;
	final public int    pid;
	final public String path;
	
	public 
	FileLockInfo( String path, String machine_id, int port )
	{
		this.path = path;
		this.pid = Integer.parseInt( machine_id.split( "@" )[ 0 ] );
		this.machine = machine_id.split( "@" )[ 1 ];
		this.port = port;
	}
	
	
	public 
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
