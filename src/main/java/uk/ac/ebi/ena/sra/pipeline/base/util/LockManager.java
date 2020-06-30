/*******************************************************************************
 * Copyright 2012 EMBL-EBI, Hinxton outstation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package uk.ac.ebi.ena.sra.pipeline.base.util;

import java.util.ArrayList;

/**
 * Manages locks to prevent activities from being executed more than once at the
 * same time.
 */
public abstract class
LockManager
{
    /**
     * Exception thrown as a result of a lock processing error.
     */
    public class 
    LockException extends Exception
    {
        private static final long serialVersionUID = 1L;


        public 
        LockException( String message )
        {
            super( message );
        }


        public 
        LockException( Throwable throwable )
        {
            super( throwable );
        }
    }

    /**
     * Callback to manage exceptions thrown during lock refresh.
     */
    public interface 
    LockCallback
    {
        /**
         * Called if an exception is thrown during lock refresh.
         * 
         * @param exception
         *            thrown during lock refresh.
         */
        void handleLockException( LockException e );
    }


    private final LockCallback      lockCallback;

    /**
     * The default lock refresh interval in milliseconds.
     */
    public final static int         DEFAULT_LOCK_REFRESH_INTERVAL = 1 * 60 * 1000;          // 1
                                                                                             // minutes

    /**
     * The default lock expiry interval in milliseconds.
     */
    public final static int         DEFAULT_LOCK_EXPIRED_INTERVAL = 5 * 60 * 1000;          // 5
                                                                                             // minutes

    /**
     * The lock refresh interval in milliseconds.
     */
    private final int               refreshInterval;

    /**
     * The lock expiry interval in milliseconds.
     */
    private final int               expiredInterval;

    /**
     * When true then the locks will be periodically refreshed.
     */
    private boolean                 lockRefresh                   = true;

    private final ArrayList<String> locks                         = new ArrayList<String>();

    private final Thread            ripper;


    /**
     * 
     * @param refreshInterval
     *            The lock refresh interval in milliseconds.
     * @param expiredInterval
     *            The lock expiry interval in milliseconds.
     */
    public 
    LockManager( LockCallback lockCallback, int refreshInterval, int expiredInterval )
    {
        this.lockCallback = lockCallback;
        this.refreshInterval = refreshInterval;
        this.expiredInterval = expiredInterval;

        this.ripper = new Thread( new Runnable() {
            public void
                run()
            {
                while( true )
                {
                    try
                    {
                        if( LockManager.this.lockRefresh )
                        {
                            refreshLocks();
                        }
                        Thread.sleep( LockManager.this.refreshInterval );
                    } catch( InterruptedException e )
                    {
                        ;
                    } catch( LockException e )
                    {
                        if( LockManager.this.lockCallback != null )
                        {
                            LockManager.this.lockCallback.handleLockException( e );
                        } else
                        {
                            throw new RuntimeException( e );
                        }
                    }
                }
            }
        } );
        this.ripper.start();
    }


    private void
    refreshLocks() throws LockException
    {
        for( String lock : locks )
        {
            refreshLock( lock );
        }
    }


    /**
     * Refreshes a lock of a stage instance.
     * 
     * @param lock
     *            The lock identifier.
     * @throws LockException
     *             In case of any error.
     */
    protected abstract void
    refreshLock( String lock ) throws LockException;


    /**
     * Locks a stage instance.
     * 
     * @param lock
     *            The lock identifier.
     * @return True if the lock was created. False if the lock already existed.
     * @throws LockException
     *             In case of any error.
     */
    public final boolean
    lock( String lock ) throws LockException
    {
        if( !locks.contains( lock ) )
        {
            locks.add( lock );
        }
        return _lock( lock, false );
    }


    /**
     * Locks a stage instance.
     * 
     * @param lock
     *            The lock identifier.
     * @param force
     *            If true then any existing lock will be removed.
     * @return True if the lock was created. False if the lock already existed.
     * @throws LockException
     *             In case of any error.
     */
    public final boolean
    lock( String lock, boolean force ) throws LockException
    {
        if( !locks.contains( lock ) )
        {
            locks.add( lock );
        }
        return _lock( lock, force );
    }


    protected abstract boolean
    _lock( String lock, boolean force ) throws LockException;


    /**
     * Returns true if the lock exists.
     * 
     * @param lock
     *            The lock identifier.
     * @return true if the lock exists.
     * @throws LockException
     *             In case of any error.
     */
    public abstract boolean
    isLocked( String lock ) throws LockException;


    /**
     * Unlocks a stage instance.
     * 
     * @param lock
     *            The lock to be released.
     * @return True if the lock was released. False if the lock had already been
     *         released.
     * @throws LockException
     *             In case of any error.
     */
    public final boolean
    unlock( String lock ) throws LockException
    {
        locks.remove( lock );
        return _unlock( lock );
    }


    protected abstract boolean _unlock( String lock ) throws LockException;


    /**
     * @return The lock refresh interval in milliseconds.
     */
    public int
    getLockRefreshInterval()
    {
        return refreshInterval;
    }


    /**
     * @return The lock expiry interval in milliseconds.
     */
    public int
    getLockExpiredInterval()
    {
        return expiredInterval;
    }


    /**
     * @return True if the locks are being refreshed (true by default).
     */
    public boolean
    isLockRefresh()
    {
        return lockRefresh;
    }


    /**
     * @param lockRefresh
     *            If true then the locks will be periodically refreshed.
     */
    public void
    setLockRefresh( boolean lockRefresh )
    {
        this.lockRefresh = lockRefresh;
    }
}
