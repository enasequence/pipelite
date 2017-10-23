package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.sql.Timestamp;

import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult.RESULT_TYPE;

public class
ExecutionInstance
{
    
    String EXEC_ID;//          NUMBER( 16, 0 ), --execution id to match logs against specific execution.
    Timestamp EXEC_START;//       DATE,          --execution start date
    Timestamp EXEC_DATE; //        DATE, 
    RESULT_TYPE EXEC_RESULT_TYPE;//  VARCHAR2( 64 ),   --FK or CHECK. proposed: 'successful execution' , 'transient error', 'permanent error', 'skipped execution'. 
    String EXEC_RESULT; //      VARCHAR2( 255 ),  -- length decreased
    String EXEC_STDOUT; //      CLOB, --VARCHAR2( 4000 ), --( or clob ) 
    String EXEC_STDERR; //      CLOB, --VARCHAR2( 4000 ), --( or clob )
    String EXEC_CMDLINE; // CLOB;    
    
    
    public
    ExecutionInstance()
    {
    }
    
    
    public
    ExecutionInstance( ExecutionInstance from )
    {
        this.EXEC_ID    = null == from.EXEC_ID ? null : new String( from.EXEC_ID );
        this.EXEC_START = null == from.EXEC_START ? null : Timestamp.from( from.EXEC_START.toInstant() );
        this.EXEC_DATE  = null == from.EXEC_DATE ? null :  Timestamp.from( from.EXEC_DATE.toInstant() );
        this.EXEC_RESULT_TYPE  = from.EXEC_RESULT_TYPE;
        this.EXEC_RESULT = null == from.EXEC_RESULT ? null : new String( from.EXEC_RESULT );
        this.EXEC_STDOUT = null == from.EXEC_STDOUT ? null : new String( from.EXEC_STDOUT );
        this.EXEC_STDERR = null == from.EXEC_STDERR ? null : new String( from.EXEC_STDERR );
        this.EXEC_CMDLINE = null == from.EXEC_CMDLINE ? null : new String( from.EXEC_CMDLINE );
    }
    
    
    @Override public boolean
    equals( Object another )
    {
        if( this == another )
            return true;
        
        if( null == another )
            return false;
        
        if( getClass() != another.getClass() )
            return false;
        
        return ( null == this.EXEC_ID ? null == ( (ExecutionInstance)another ).EXEC_ID : this.EXEC_ID.equals( ( (ExecutionInstance)another ).EXEC_ID ) )
            && ( null == this.EXEC_START ? null == ( (ExecutionInstance)another ).EXEC_START : this.EXEC_START.equals( ( (ExecutionInstance)another ).EXEC_START ) )
            && ( null == this.EXEC_DATE ? null == ( (ExecutionInstance)another ).EXEC_DATE : this.EXEC_DATE.equals( ( (ExecutionInstance)another ).EXEC_DATE ) )
            && ( null == this.EXEC_RESULT_TYPE ? null == ( (ExecutionInstance)another ).EXEC_RESULT_TYPE : this.EXEC_RESULT_TYPE.equals( ( (ExecutionInstance)another ).EXEC_RESULT_TYPE ) )
            && ( null == this.EXEC_RESULT ? null == ( (ExecutionInstance)another ).EXEC_RESULT : this.EXEC_RESULT.equals( ( (ExecutionInstance)another ).EXEC_RESULT ) )
            && ( null == this.EXEC_STDOUT ? null == ( (ExecutionInstance)another ).EXEC_STDOUT : this.EXEC_STDOUT.equals( ( (ExecutionInstance)another ).EXEC_STDOUT ) )
            && ( null == this.EXEC_STDERR ? null == ( (ExecutionInstance)another ).EXEC_STDERR : this.EXEC_STDERR.equals( ( (ExecutionInstance)another ).EXEC_STDERR ) )
            && ( null == this.EXEC_CMDLINE ? null == ( (ExecutionInstance)another ).EXEC_CMDLINE : this.EXEC_CMDLINE.equals( ( (ExecutionInstance)another ).EXEC_CMDLINE ) );
    }
    
    
    
    public String
    getExecutionId()
    {
        return EXEC_ID;
    }
    
    
    public void
    setExceutionId( String execution_id )
    {
        EXEC_ID = execution_id;
    }
    
    
    public Timestamp
    getStart()
    {
        return EXEC_START;
    }
    
    
    public void
    setStartTime( Timestamp start )
    {
        EXEC_START = start;
    }
    
    
    public Timestamp
    getFinish()
    {
        return EXEC_DATE;
    }
    
    
    public void
    setFinishTime( Timestamp finish )
    {
        EXEC_DATE = finish;
    }
    
    
    public RESULT_TYPE
    getResultType()
    {
        return EXEC_RESULT_TYPE;
    }
    
    
    public void
    setResultType( RESULT_TYPE result_type )
    {
        EXEC_RESULT_TYPE = result_type;
    }
    
    
    public String
    getResult()
    {
        return EXEC_RESULT;
    }
    
    
    public void
    setResult( String result )
    {
        EXEC_RESULT = result;
    }
    
    
    public String
    getStdout()
    {
        return EXEC_STDOUT;
    }
    
    
    public void
    setStdout( String stdout )
    {
        EXEC_STDOUT = stdout;
    }
    
    
    public String
    getStderr()
    {
        return EXEC_STDERR;
    }
    
    
    public void
    setStderr( String stderr )
    {
        EXEC_STDERR = stderr;
    }


	public String 
	getCmdLine() 
	{
		return EXEC_CMDLINE;
	}


	public void 
	setCmdLine( String exec_cmdline ) 
	{
		EXEC_CMDLINE = exec_cmdline;
	}
}
