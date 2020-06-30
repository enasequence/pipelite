package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
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
        super( corePoolSize, corePoolSize, 0, TimeUnit.DAYS, new SynchronousQueue<Runnable>() );
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
        try
        {
            super.execute( runnable );
        } catch( RejectedExecutionException ree )
        {
            synchronized( running )
            {
                running.remove( id, runnable );
            }
            throw ree;
        }
    }


    @Override protected void 
    afterExecute( Runnable r, Throwable t )
    {
        super.afterExecute( r, t );
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
    }


    @Override protected void 
    beforeExecute( Thread t, Runnable r )
    {
        super.beforeExecute( t, r );
    }
}
    
