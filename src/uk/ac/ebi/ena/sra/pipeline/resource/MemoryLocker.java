package uk.ac.ebi.ena.sra.pipeline.resource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class 
MemoryLocker implements ResourceLocker
{
    
    Map<String, String> stage_locks = Collections.synchronizedMap( new HashMap<String, String>( 256 ) );    

    @Override public boolean
    lock( StageResourceLock rl )
    {
        return null == stage_locks.putIfAbsent( rl.getLockId(), rl.getLockOwner() );
    }


    @Override public boolean
    unlock( StageResourceLock rl )
    {
        return stage_locks.remove( rl.getLockId(), rl.getLockOwner() );
    }

    
    @Override public boolean
    is_locked( StageResourceLock rl )
    {
        return stage_locks.replace( rl.getLockId(), rl.getLockOwner(), rl.getLockOwner() ); 
    }


    Map<String, String> process_locks = Collections.synchronizedMap( new HashMap<String, String>( 256 ) );    

    @Override public boolean
    lock( ProcessResourceLock rl )
    {
        return null == process_locks.putIfAbsent( rl.getLockId(), rl.getLockOwner() );
    }


    @Override public boolean
    unlock( ProcessResourceLock rl )
    {
        return process_locks.remove( rl.getLockId(), rl.getLockOwner() );
    }

    
    @Override public boolean
    is_locked( ProcessResourceLock rl )
    {
        return process_locks.replace( rl.getLockId(), rl.getLockOwner(), rl.getLockOwner() ); 
    }
}
