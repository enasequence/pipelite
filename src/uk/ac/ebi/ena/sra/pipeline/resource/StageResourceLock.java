package uk.ac.ebi.ena.sra.pipeline.resource;

public class 
StageResourceLock extends ResourceLock 
{
    public 
    StageResourceLock( String lock_owner, String process_id, String stage_name )
    {
        super( lock_owner, process_id, stage_name );
    }

    
	public 
	StageResourceLock( String lock_owner, String stage_name )
	{
		super( lock_owner, stage_name );
	}
}
