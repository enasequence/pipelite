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
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
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

@Configuration
@ConfigurationProperties(prefix = "pipelite.datasource")
@Data
@EnableJpaRepositories(
    basePackages = "pipelite.repository",
    entityManagerFactoryRef = "pipeliteEntityManager",
    transactionManagerRef = "pipeliteTransactionManager")
@Flogger
public class RepositoryConfiguration {

  private static final int DEFAULT_MINIMUM_IDLE = 10;
  private static final int DEFAULT_MAXIMUM_POOL_SIZE = 25;
  private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofMinutes(1);

  @Autowired Environment environment;

  private String driverClassName;
  private String url;
  private String username;
  private String password;
  private String ddlAuto;
  private String dialect;
  private Integer minimumIdle;
  private Integer maximumPoolSize;
  private Duration connectionTimeout;
  /** Uses an in memory database if a valid repository configuration has not been provided. */
  private boolean test;

  // Better error reporting for required properties.
  private boolean checkRequiredProperties() {
    boolean isValid = true;
    if (driverClassName == null || driverClassName.trim().isEmpty()) {
      log.atSevere().log("Missing required pipelite property: pipelite.repository.driverClassName");
      isValid = false;
    }
    if (url == null) {
      log.atSevere().log("Missing required pipelite property: pipelite.repository.url");
      isValid = false;
    }
    if (username == null) {
      log.atSevere().log("Missing required pipelite property: pipelite.repository.username");
      isValid = false;
    }
    if (password == null) {
      log.atSevere().log("Missing required pipelite property: pipelite.repository.password");
      isValid = false;
    }

    if (!isValid && (test || isTestProfile())) {
      log.atSevere().log("Using an in memory database unsuitable for production purposes.");
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

  @Bean
  @Primary
  public LocalContainerEntityManagerFactoryBean pipeliteEntityManager() {
    if (!checkRequiredProperties()) {
      throw new RuntimeException("Missing required pipelite properties: pipelite.repository.*");
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
    hikariConfig.setPoolName("pipelite");
    hikariConfig.setAutoCommit(false);
    return new HikariDataSource(hikariConfig);
  }

  @Primary
  @Bean("pipeliteTransactionManager")
  public PlatformTransactionManager pipeliteTransactionManager() {
    JpaTransactionManager transactionManager = new JpaTransactionManager();
    transactionManager.setEntityManagerFactory(pipeliteEntityManager().getObject());
    return transactionManager;
  }

  private boolean isTestProfile() {
    return Arrays.asList(environment.getActiveProfiles()).contains("pipelite-test");
  }
}
