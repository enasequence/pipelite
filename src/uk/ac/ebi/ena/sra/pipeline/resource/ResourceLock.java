package uk.ac.ebi.ena.sra.pipeline.resource;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class 
ResourceLock
{
    private String[] parts;
    private String   pipeline_name;
    protected String separator = "/";
    
    
    public String
    getSeparator()
    {
        return this.separator;
    }
    
    public
    ResourceLock( String pipeline_name, String...parts )
    {
        this.parts = parts;
        this.pipeline_name = pipeline_name;
    }
    
    
    public String[] 
    getParts()
    {
        return parts;
    };
    
    
    public String
    getLockId()
    {
        return Stream.of( parts ).collect( Collectors.joining( getSeparator() ) );
    }
    
    
    public String 
    getLockOwner()
    {
        return pipeline_name;
    }

    
    @Override public int
    hashCode()
    {
        return getParts().hashCode();
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
        return String.format( "%2$s: %1$s", getLockId(), getLockOwner() );
    }
}
