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
import java.util.HashMap;
import javax.sql.DataSource;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
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

  private static final int DEFAULT_MAX_ACTIVE = 10;

  private String driverClassName;
  private String url;
  private String username;
  private String password;
  private String ddlAuto;
  private String dialect;
  private Integer maxActive;

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
    em.setPackagesToScan(new String[] {"pipelite.entity"});

    HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
    em.setJpaVendorAdapter(vendorAdapter);
    HashMap<String, Object> properties = new HashMap<>();
    if (ddlAuto != null) {
      properties.put("hibernate.hbm2ddl.auto", ddlAuto);
    }
    if (dialect != null) {
      properties.put("hibernate.dialect", dialect);
    }
    em.setJpaPropertyMap(properties);
    return em;
  }

  @Primary
  @Bean("pipeliteDataSource")
  public DataSource pipeliteDataSource() {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDriverClassName(driverClassName);
    hikariConfig.setJdbcUrl(url);
    hikariConfig.setUsername(username);
    hikariConfig.setPassword(password);
    if (maxActive == null || maxActive < 1) {
      maxActive = DEFAULT_MAX_ACTIVE;
    }
    hikariConfig.setMaximumPoolSize(maxActive);
    hikariConfig.setPoolName("pipeliteConnectionPool");
    hikariConfig.setAutoCommit(false);
    HikariDataSource dataSource = new HikariDataSource(hikariConfig);
    return dataSource;
  }

  @Primary
  @Bean("pipeliteTransactionManager")
  public PlatformTransactionManager pipeliteTransactionManager() {
    JpaTransactionManager transactionManager = new JpaTransactionManager();
    transactionManager.setEntityManagerFactory(pipeliteEntityManager().getObject());
    return transactionManager;
  }
}
