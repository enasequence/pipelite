package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class 
TaggedPoolExecutor extends ThreadPoolExecutor
{
    Logger log = Logger.getLogger( this.getClass() );
    
    Map<Object, Runnable> running = Collections.synchronizedMap( new WeakHashMap<Object, Runnable>() );
    
    public 
    TaggedPoolExecutor( int corePoolSize )
    {
        super( corePoolSize, corePoolSize, 1, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>( corePoolSize ) );
    }

    
    public void
    execute( Object id, Runnable runnable )
    {
        synchronized( running )
        {
            if( running.containsKey( id ) )
                return;
            running.put( id, runnable );
        }
        
        super.execute( runnable );
    }


    protected void 
    afterExecute( Runnable r, Throwable t )
    {
        synchronized( running )
        {
            if( !running.containsValue( r ) )
                log.warn( "Attempted to remove not existing value" );
            
            for( Iterator<Entry<Object, Runnable>> i = running.entrySet().iterator(); i.hasNext(); )
            {
                Entry<?, ?> e = i.next();
                if( e.getValue() == r )
                {
                    i.remove();
                    break;
                }
            }
        }
        super.afterExecute( r, t );
    }


    protected void 
    beforeExecute( Thread t, Runnable r )
    {
        super.beforeExecute( t, r );
    }
}
    
