package uk.ac.ebi.ena.sra.pipeline.launcher;

import uk.ac.ebi.ena.sra.pipeline.storage.ProcessLogBean;



public class 
StageTaskException extends Exception 
{
	private static final long serialVersionUID = 1L;

	ProcessLogBean log_bean;
	
	
	public 
	StageTaskException( String message, Throwable cause, ProcessLogBean log_bean )
	{
		super( message, cause );
		this.log_bean = log_bean;
	}
	
	
	public void
	setLogBean( ProcessLogBean log_bean )
	{
		this.log_bean = log_bean;
	}
	
	
	public ProcessLogBean
	getLogBean()
	{
		return log_bean;
	}
	
}
