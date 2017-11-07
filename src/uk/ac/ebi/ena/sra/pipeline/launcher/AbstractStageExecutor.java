package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.util.List;

import org.apache.log4j.Logger;


public abstract class 
AbstractStageExecutor implements StageExecutor
{
    protected Logger log = Logger.getLogger( this.getClass() );
    protected final String PIPELINE_NAME;
    protected final ResultTranslator TRANSLATOR;
    protected boolean reprocess_processed;
    private int redo;
    
    
    public 
    AbstractStageExecutor( String pipeline_name, ResultTranslator translator )
    {
        this.PIPELINE_NAME = pipeline_name;
        this.TRANSLATOR    = translator;
    }


    @SuppressWarnings( "unchecked" ) public <T extends AbstractStageExecutor> T
    setRedoCount( int redo )
    {
        this.redo = redo;
        return (T) this;
    }
    
    
    @Override public EvalResult
    can_execute( StageInstance instance )
    {
        // disabled stage
        if( !instance.isEnabled() )
            return EvalResult.StageTerminal;
        
        ExecutionInstance ei = instance.getExecutionInstance();
        
        //check permanent errors
        if( null != ei && null != ei.getFinish() )
        {
    		switch ( ei.getResultType() )
    		{
    		case PERMANENT_ERROR:
    			return EvalResult.ProcessTerminal;
			default:
				return ei.getResultType().canReprocess() ? EvalResult.StageTransient : EvalResult.StageTerminal;
    		}
        }
        
        return EvalResult.StageTransient;
    }


	protected void 
	appendProperties(List<String> p_args, String...properties_to_pass) 
	{
		for( String name : properties_to_pass )
	    {
	    	for( Object p_name : System.getProperties().keySet() )
	    		if( String.valueOf( p_name ).startsWith( name ) )
	    			p_args.add( String.format( "-D%s=%s", p_name, System.getProperties().get( p_name ) ) );
	    }
	}
}