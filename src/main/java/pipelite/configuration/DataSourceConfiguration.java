/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.configuration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Logger;
import javax.sql.DataSource;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import pipelite.metrics.DataSourceMetrics;

@Configuration
@ConfigurationProperties(prefix = "pipelite.datasource")
@Data
@EnableJpaRepositories(
    basePackages = "pipelite.repository",
    entityManagerFactoryRef = "pipeliteEntityManager",
    transactionManagerRef = "pipeliteTransactionManager")
@Flogger
public class DataSourceConfiguration {

  private static final int DEFAULT_MINIMUM_IDLE = 10;
  private static final int DEFAULT_MAXIMUM_POOL_SIZE = 25;
  private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofMinutes(5);

  @Autowired ProfileConfiguration profileConfiguration;
  @Autowired DataSourceMetrics dataSourceMetrics;

  private String driverClassName;
  private String url;
  private String username;
  private String password;
  private String ddlAuto;
  private String dialect;
  private Integer minimumIdle;
  private Integer maximumPoolSize;
  private Duration connectionTimeout;
  /** Uses an in-memory database if a valid datasource has not been provided. */
  private boolean test;

  // Better error reporting for required properties.
  private boolean checkRequiredProperties() {
    boolean isValid = true;
    if (!isDefinedValue(driverClassName)) {
      log.atSevere().log("Missing required pipelite property: pipelite.datasource.driverClassName");
      isValid = false;
    }
    if (!isDefinedValue(url)) {
      log.atSevere().log("Missing required pipelite property: pipelite.datasource.url");
      isValid = false;
    }
    if (!isDefinedValue(username)) {
      log.atSevere().log("Missing required pipelite property: pipelite.datasource.username");
      isValid = false;
    }
    if (!isDefinedValue(password)) {
      log.atSevere().log("Missing required pipelite property: pipelite.datasource.password");
      isValid = false;
    }

    if (!isValid && (test || profileConfiguration.isTestProfile())) {
      log.atSevere().log("Using an in-memory database unsuitable for production purposes.");
      this.driverClassName = "org.hsqldb.jdbc.JDBCDriver";
      this.url = "jdbc:hsqldb:mem:testdb;DB_CLOSE_DELAY: -1";
      this.username = "sa";
      this.password = "";
      this.minimumIdle = DEFAULT_MINIMUM_IDLE;
      this.maximumPoolSize = DEFAULT_MAXIMUM_POOL_SIZE;
      this.connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
      this.ddlAuto = "create";
      this.dialect = "org.hibernate.dialect.HSQLDialect";
      return true;
    }

    return isValid;
  }

  private boolean isDefinedValue(String value) {
    return value != null && !value.trim().isEmpty();
  }

  @Bean
  @Primary
  public LocalContainerEntityManagerFactoryBean pipeliteEntityManager() {
    if (!checkRequiredProperties()) {
      throw new RuntimeException("Missing required pipelite properties: pipelite.datasource.*");
    }

    LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
    em.setDataSource(pipeliteDataSource());
    em.setPackagesToScan("pipelite.entity");

    HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
    em.setJpaVendorAdapter(vendorAdapter);
    HashMap<String, Object> properties = new HashMap<>();
    if (ddlAuto != null) {
      properties.put("hibernate.hbm2ddl.auto", ddlAuto);
    }
    if (dialect != null) {
      properties.put("hibernate.dialect", dialect);
    }
    // properties.put("hibernate.jdbc.time_zone", ZoneId.systemDefault());
    em.setJpaPropertyMap(properties);
    return em;
  }

  @Primary
  @Bean("pipeliteDataSource")
  public DataSource pipeliteDataSource() {
    if (minimumIdle == null || minimumIdle < 1) {
      minimumIdle = DEFAULT_MINIMUM_IDLE;
    }
    if (maximumPoolSize == null || maximumPoolSize < 1) {
      maximumPoolSize = DEFAULT_MAXIMUM_POOL_SIZE;
    }
    if (connectionTimeout == null) {
      connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
    }
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDriverClassName(driverClassName);
    hikariConfig.setJdbcUrl(url);
    hikariConfig.setUsername(username);
    hikariConfig.setPassword(password);
    hikariConfig.setMinimumIdle(minimumIdle);
    hikariConfig.setMaximumPoolSize(maximumPoolSize);
    hikariConfig.setConnectionTimeout(connectionTimeout.toMillis());
    hikariConfig.setLeakDetectionThreshold(5000);
    hikariConfig.setPoolName("pipelite");
    hikariConfig.setAutoCommit(false);
    return new PipeliteDataSource(new HikariDataSource(hikariConfig), dataSourceMetrics);
  }

  @Primary
  @Bean("pipeliteTransactionManager")
  public PlatformTransactionManager pipeliteTransactionManager() {
    JpaTransactionManager transactionManager = new JpaTransactionManager();
    transactionManager.setEntityManagerFactory(pipeliteEntityManager().getObject());
    return transactionManager;
  }

  public static boolean isTestProfile(Environment environment) {
    return Arrays.asList(environment.getActiveProfiles()).contains("test");
  }

  /** Data source that updates data source metrics. */
  public static class PipeliteDataSource implements DataSource {
    private final DataSource dataSource;
    private final DataSourceMetrics dataSourceMetrics;

    public PipeliteDataSource(DataSource dataSource, DataSourceMetrics dataSourceMetrics) {
      this.dataSource = dataSource;
      this.dataSourceMetrics = dataSourceMetrics;
    }

    @Override
    public Connection getConnection() throws SQLException {
      ZonedDateTime startTime = ZonedDateTime.now();
      Connection connection = dataSource.getConnection();
      dataSourceMetrics
          .getConnectionTimer()
          .record(Duration.between(startTime, ZonedDateTime.now()));
      return connection;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return dataSource.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return dataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
      dataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      dataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return dataSource.getLoginTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return dataSource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return dataSource.isWrapperFor(iface);
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return dataSource.getParentLogger();
    }

    @Override
    public ConnectionBuilder createConnectionBuilder() throws SQLException {
      return dataSource.createConnectionBuilder();
    }
  }
}
