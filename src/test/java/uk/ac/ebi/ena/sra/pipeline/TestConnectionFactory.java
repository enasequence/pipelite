package uk.ac.ebi.ena.sra.pipeline;

import uk.ac.ebi.ena.sra.pipeline.configuration.OracleHeartBeatConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class TestConnectionFactory {
  public static Connection createConnection() {
    return createConnection(
        "era",
        "eradevt1",
        "jdbc:oracle:thin:@ (DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = ora-dlvm5-008.ebi.ac.uk)(PORT = 1521))) (CONNECT_DATA = (SERVICE_NAME = VERADEVT) (SERVER = DEDICATED)))");
  }

  public static Connection createConnection(String user, String passwd, String url) {

    Properties props = new Properties();
    props.put("user", user);
    props.put("password", passwd);
    props.put("SetBigStringTryClob", "true");

    try {
      Class.forName("oracle.jdbc.driver.OracleDriver");
      Connection connection =
          new OracleHeartBeatConnection(DriverManager.getConnection(url, props));
      connection.setAutoCommit(false);
      return connection;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
