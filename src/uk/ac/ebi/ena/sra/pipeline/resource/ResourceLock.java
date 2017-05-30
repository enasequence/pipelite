package uk.ac.ebi.ena.sra.pipeline.resource;


public abstract class 
ResourceLock
{
    private String lock_id;
    private String lock_owner;
    
    
    public
    ResourceLock( String lock_id, String lock_owner )
    {
        this.lock_id = lock_id;
        this.lock_owner = lock_owner;
    }
    
    
    public String 
    getLockId()
    {
        return lock_id;
    };
    
    
    public String 
    getLockOwner()
    {
        return lock_owner;
    }

    
    @Override public int
    hashCode()
    {
        return getLockId().hashCode();
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
        
        return ( getLockId().equals( ( (ResourceLock)another ).getLockId() ) ) 
            && ( getLockOwner().equals( ( ( ResourceLock)another ).getLockOwner() ) );
    }
    
    
    @Override public String 
    toString()
    {
        return String.format( "%s: %s", getLockId(), getLockOwner() );
    }
}
