package uk.ac.ebi.ena.sra.pipeline.filelock;

public class 
FileLockException extends RuntimeException
{
	private static final long serialVersionUID = 1L;
	private FileLockInfo info;


    public 
    FileLockException( FileLockInfo info, String message, Throwable cause )
    {
        super( message, cause );
        this.info = info;
    }

    
    public FileLockInfo 
    getInfo()
    {
        return info;
    }

    
    public void 
    setInfo( FileLockInfo info )
    {
        this.info = info;
    }
    
    
	public String 
	toString()
	{
		return String.format( "%s: %s", getMessage(), info.path );
	}
}