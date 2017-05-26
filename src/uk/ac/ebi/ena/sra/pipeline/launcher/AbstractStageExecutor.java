package uk.ac.ebi.ena.sra.pipeline.launcher;

import org.apache.log4j.Logger;

import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult.RESULT_TYPE;


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
        
        // redo counter exceeded
//        if( instance.getExecutionCount() > redo )
//            return EvalResult.ProcessTerminal;
                               
        ExecutionInstance ei = instance.getExecutionInstance();
        
        //check permanent errors
        if( null != ei && null != ei.getFinish() )
        {
//        	if( reprocess_processed )
//        		return EvalResult.StageTransient;
//        	else
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
}