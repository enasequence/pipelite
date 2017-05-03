package uk.ac.ebi.ena.sra.pipeline.resource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class 
MemoryLocker implements ResourceLocker
{
    
    Map<String, String> locks = Collections.synchronizedMap( new HashMap<String, String>( 256 ) );    

    @Override public boolean
    lock( ResourceLock rl )
    {
        return null == locks.putIfAbsent( rl.getLockId(), rl.getLockOwner() );
    }


    @Override public boolean
    unlock( ResourceLock rl )
    {
        return locks.remove( rl.getLockId(), rl.getLockOwner() );
    }

    
    @Override public boolean
    is_locked( ResourceLock rl )
    {
        return locks.replace( rl.getLockId(), rl.getLockOwner(), rl.getLockOwner() ); 
    }

}
