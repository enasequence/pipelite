package uk.ac.ebi.ena.sra.pipeline.configuration;

import java.sql.Connection;

/**
 * An oracle database connection (session) with selects executed on regular intervals to help to keep long running sessions alive.
 * 
 * @see java.sql.Connection;
 */
public class 
OracleHeartBeatConnection extends HeartBeatConnection
{
    final static String SELECT_QUERY = "select 1 from dual";

    /**
     * @param connection The database connection.
     */    
    public
    OracleHeartBeatConnection( Connection connection )
    {
        super( connection, SELECT_QUERY );
    }    

    /**
     * @param connection The database connection.
     * @param heartbeat_interval The interval to execute regular selects.
     */        
    public 
    OracleHeartBeatConnection( Connection connection, int heartbeat_interval )
    {
        super( connection, heartbeat_interval, SELECT_QUERY );
    }
}
