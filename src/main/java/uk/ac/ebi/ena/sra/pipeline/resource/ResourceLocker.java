package uk.ac.ebi.ena.sra.pipeline.resource;


public interface 
ResourceLocker
{
    public boolean lock( StageResourceLock rl );
    public boolean unlock( StageResourceLock rl );
    public boolean is_locked( StageResourceLock rl );
    
    public boolean lock( ProcessResourceLock rl );
    public boolean unlock( ProcessResourceLock rl );
    public boolean is_locked( ProcessResourceLock rl );

}
