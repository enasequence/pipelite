package uk.ac.ebi.ena.sra.pipeline.configuration;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

public class OracleHeartBeatConnectionTest
{
    @Test
    public void
    test() throws ClassNotFoundException, SQLException
    {
        Properties p = new Properties();
        p.put( "user", "era_reader" );
        p.put( "password", "reader" );
        p.put( "SetBigStringTryClob", "true" );

        Class.forName( "oracle.jdbc.driver.OracleDriver" );
 
        Connection connection = DriverManager.getConnection( "jdbc:oracle:thin:@(DESCRIPTION =" 
                + "(ADDRESS_LIST = "
                + "(ADDRESS = "
                  + "(PROTOCOL = TCP)"
                    + "(HOST = ora-dlvm5-008.ebi.ac.uk)"
                    + "(PORT = 1521)"
                + ")"
            + ")"
                + "(CONNECT_DATA = "
                   +  "(SERVICE_NAME = VERADEVT)"
                   +  "(SERVER = SHARED)"
                + ")"
            + ")", p );
                
        connection.setAutoCommit( false );
        
        OracleHeartBeatConnection hc = new OracleHeartBeatConnection( connection, 4000 ); 

        assertTrue( hc.isValid( 0 ) );

        hc.rollback();

        hc.close();
    }
}
