package uk.ac.ebi.ena.sra.pipeline.mock.schedule;

import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;

/*
 * SAMPLE CommitStatus. Must be substituted
 */
public enum 
ExecutionResults implements ExecutionResult
{
	OK( null, 0, null, RESULT_TYPE.SUCCESS ),
	SKIPPED( "SKIPPED", 51, Exception.class, RESULT_TYPE.SUCCESS ),
    UNKNOWN_FAILURE( "UNKNOWN_FAILURE", 53, Throwable.class, RESULT_TYPE.TRANSIENT_ERROR );
    
	
    ExecutionResults( String           message,
    		      	  int              exit_code,
    		          Class<?>         cause,
    		          RESULT_TYPE      type )
    {
    	this.message    = message;
    	this.type       = type;
    	this.exit_code  = (byte)exit_code;
    	this.cause      = cause;
    }
    
    
    final String   message;
    final RESULT_TYPE type;
    final byte     exit_code;
    final Class<?> cause;
    
    
    
	@Override
	public byte 
	getExitCode() 
	{
		return exit_code;
	}


	@Override
	public Class<Throwable>
	getCause()
	{
		return (Class<Throwable>)cause;
	}


	@Override
	public String 
	getMessage() 
	{
		return message;
	}


	@Override
	public RESULT_TYPE 
	getType() 
	{
		return type;
	}
}
