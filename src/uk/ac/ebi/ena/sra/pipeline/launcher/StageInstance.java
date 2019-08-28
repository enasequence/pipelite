package uk.ac.ebi.ena.sra.pipeline.launcher;

import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;


public class
StageInstance
{
    private int    exec_cnt;
    private String process_id;
    private String stage_name;
    private String pipeline_name;
    private String depends_on;
    private boolean enabled;
    private ExecutorConfig resource_config[];
    private ExecutionInstance execution_instance = new ExecutionInstance();
    
    
    
    public 
    StageInstance()
    {
        
    }
    
    
    public 
    StageInstance( StageInstance from )
    {
        this.exec_cnt = from.exec_cnt;
        this.process_id = from.process_id;
        this.stage_name = from.stage_name;
        this.pipeline_name = from.pipeline_name;
        this.depends_on = from.depends_on;
        this.enabled = from.enabled;
        this.resource_config = from.resource_config;
        this.execution_instance = new ExecutionInstance( from.execution_instance );
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
        
        return    ( null == getPipelineName() ? null == ( (StageInstance)another ).getPipelineName() : getPipelineName().equals( ( (StageInstance)another ).getPipelineName() ) ) 
               && ( null == getProcessID() ? null == ( (StageInstance)another ).getProcessID() : getProcessID().equals( ( (StageInstance)another ).getProcessID() ) )
               && ( null == getStageName() ? null == ( (StageInstance)another ).getStageName() : getStageName().equals( ( (StageInstance)another ).getStageName() ) )
               && ( getExecutionCount() == ( (StageInstance)another ).getExecutionCount() )
               && ( null == getDependsOn() ? null == ( (StageInstance)another ).getDependsOn() : getDependsOn().equals( ( (StageInstance)another ).getDependsOn() ) )
               && ( isEnabled() == ( (StageInstance)another ).isEnabled() )
//TODO               
/* ? */        && ( this.resource_config == ( (StageInstance)another ).resource_config )
               && ( null == getExecutionInstance() ? null == ( (StageInstance)another ).getExecutionInstance() : getExecutionInstance().equals( ( (StageInstance)another ).getExecutionInstance() ) );
    }
    
    
    
    @Override public String 
    toString()
    {
        return String.format( "[%s]-[%s]-[%s]-[%d]", getPipelineName(), getProcessID(), getStageName(), getExecutionCount() );
    }
    
    
    public <T extends ExecutorConfig> T
        getResourceConfig( Class<? extends ExecutorConfig> klass )
    {
        if( null == resource_config )
            return null;

        for( ExecutorConfig r : resource_config )
        {
            try
            {
                return (T)klass.cast( r );
            }catch( ClassCastException cce )
            {
                ;
            }
        }
        return null;
    }
    
    
    public void
    setResourceConfig( ExecutorConfig...resource_config )
    {
        this.resource_config = resource_config;
    }
    
    
    public boolean
    isEnabled()
    {
        return enabled;
    }
        
    
    public void
    setEnabled( boolean enabled )
    {
        this.enabled = enabled;
    }

   
    public int
    getExecutionCount()
    {
        return exec_cnt;
    }


    public void 
    setExecutionCount( int exec_cnt )
    {
        this.exec_cnt = exec_cnt;
    }

    
    public String 
    getPipelineName()
    {
        return pipeline_name;
    }


    public void 
    setPipelineName( String pipeline_name )
    {
        this.pipeline_name = pipeline_name;
    }

    
    public String 
    getProcessID()
    {
        return process_id;
    }


    public void 
    setProcessID( String process_id )
    {
        this.process_id = process_id;
    }

    
    public String 
    getStageName()
    {
        return stage_name;
    }


    public void 
    setStageName( String stage_name )
    {
        this.stage_name = stage_name;
    }


    public void 
    setDependsOn( String depends_on )
    {
        this.depends_on = depends_on;
    }


    public String 
    getDependsOn()
    {
        return depends_on;
    }


    public void
    setExecutionInstance( ExecutionInstance ei )
    {
        execution_instance = ei;
    }
    
    
    public ExecutionInstance
    getExecutionInstance()
    {
        return execution_instance;
    }

}
