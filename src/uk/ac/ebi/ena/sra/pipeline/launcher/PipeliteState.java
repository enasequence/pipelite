package uk.ac.ebi.ena.sra.pipeline.launcher;

public class 
PipeliteState
{
    public 
    enum State
    {
        ACTIVE,
        RUNNING,   //cannot be selected
        INACTIVE,  //cannot be selected
        COMPLETED, //cannot be selected
        CANCELLED, //cannot be selected
        FAILED;    //cannot be selected
    }
    
    
    String pipeline_name;   //  VARCHAR2( 64 ) NOT NULL,
    String process_id;      //  VARCHAR2( 15 ),
    int    priority;        //  NUMBER(1,0) NOT NULL,
    State  state;           //  VARCHAR2( 16 ) DEFAULT 'ACTIVE' NOT NULL ENABLE, --Wether the process avaliable to execution or not
    String process_comment; //  VARCHAR2( 4000 ), -- user comment
    int    exec_cnt;
    
    
    //TODO Override hashCode();
    
    
    @Override public boolean
    equals( Object another )
    {
        if( this == another )
            return true;
        
        if( null == another )
            return false;
        
        if( getClass() != another.getClass() )
            return false;
        
        return    ( null == getPipelineName() ? null  == ( (PipeliteState)another ).getPipelineName() : getPipelineName().equals( ( (PipeliteState)another ).getPipelineName() ) ) 
               && ( null == getProcessId() ? null == ( (PipeliteState)another ).getProcessId() : getProcessId().equals( ( (PipeliteState)another ).getProcessId() ) )
               && ( getExecCount() == ( (PipeliteState)another ).getExecCount() )
               && ( getPriority() ==( (PipeliteState)another ).getPriority() )
               && ( null == getProcessComment() ? null == ( (PipeliteState)another ).getProcessComment() : getProcessComment().equals( ( (PipeliteState)another ).getProcessComment() ) );
    }
    
    
    @Override public String 
    toString()
    {
        return String.format( "%s-%s-%s-%s[%36s]", getPipelineName(), getProcessId(), getPriority(), getState(), getProcessComment() );
    }
    
    
    
    public 
    PipeliteState()
    {
        
    }
    
    
    public 
    PipeliteState( String pipeline_name, String process_id )
    {
        this.pipeline_name = pipeline_name;
        this.process_id    = process_id;
    }
    
    
    public String
    getPipelineName()
    {
        return this.pipeline_name;
    }
    
    
    public void
    setPipelineName( String pIPELINE_NAME )
    {
        this.pipeline_name = pIPELINE_NAME;
    }
    
    
    public String
    getProcessId()
    {
        return this.process_id;
    }
    
    
    public void
    setProcessId( String pROCESS_ID )
    {
        this.process_id = pROCESS_ID;
    }
    
    
    public int
    getPriority()
    {
        return this.priority;
    }
    
    
    public void
    setPriority( int priority )
    {
        this.priority = priority;
    }
    
    
    public State
    getState()
    {
        return this.state;
    }
    
    
    public void
    setState( State state )
    {
        this.state = state;
    }
    
    
    public String
    getProcessComment()
    {
        return this.process_comment;
    }
    
    
    public void
    setProcessComment( String PROCESS_COMMENT )
    {
        this.process_comment = PROCESS_COMMENT;
    }
    
    
    public int
    getExecCount()
    {
        return exec_cnt;
    }
    
    
    public void
    setExecCount( int exec_cnt )
    {
        this.exec_cnt = exec_cnt;
    }
}
