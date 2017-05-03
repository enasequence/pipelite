package uk.ac.ebi.ena.sra.pipeline.resource;


public interface 
ResourceLocker
{
    public boolean lock( ResourceLock rl );
    public boolean unlock( ResourceLock rl );
    public boolean is_locked( ResourceLock rl );
}
