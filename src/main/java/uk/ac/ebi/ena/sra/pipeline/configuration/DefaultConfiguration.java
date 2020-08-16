/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import pipelite.task.result.resolver.TaskExecutionResultResolver;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;

class ConfigurationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public ConfigurationException(String value) {
    super(value);
  }

  public ConfigurationException(Throwable value) {
    super(value);
  }
}

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
@interface PipeliteProperty {

  boolean required() default true;
}

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
@interface PipelitePropertyIntRange {

  int[] range() default {};
}

public enum DefaultConfiguration {
  CURRENT("pipelite");
  final String prefix, f_name;
  static final String SEPARATOR = ".";
  final Properties p;

  DefaultConfiguration(String prefix) {
    this.prefix = prefix;

    f_name = System.getProperty(prefix);

    if (null == f_name || f_name.trim().length() == 0) {
      throw new ConfigurationException(String.format("Null or empty value for mode %s", prefix));
    }

    p = new Properties();
    try {
      p.load(getClass().getResourceAsStream(f_name));
      checkProperties();
    } catch (ConfigurationException ce) {
      throw ce;
    } catch (Exception e) {
      try {
        p.load(new FileInputStream(new File(f_name)));
        checkProperties();
      } catch (ConfigurationException ce) {
        throw ce;
      } catch (Exception fe) {
        fe.printStackTrace();
      }
    }
  }

  private void checkProperties()
      throws IllegalAccessException, IllegalArgumentException,
          NoSuchMethodException, SecurityException {
    List<String> result = new ArrayList<>();
    for (Method m : this.getClass().getDeclaredMethods()) {
      try {
        {
          PipeliteProperty p = m.getAnnotation(PipeliteProperty.class);
          if (null != p && p.required()) {
            this.getClass().getDeclaredMethod(m.getName()).invoke(this);
          }
        }

        {
          PipelitePropertyIntRange p = m.getAnnotation(PipelitePropertyIntRange.class);
          if (null != p && p.range().length > 0) {
            for (int value : p.range()) {
              this.getClass().getDeclaredMethod(m.getName(), int.class).invoke(this, value);
            }
          }
        }
      } catch (InvocationTargetException ite) {
        result.add(ite.getTargetException().getMessage());
      }
    }

    if (!result.isEmpty()) {
      throw new ConfigurationException(String.join("%n", result));
    }
  }

  public String getConfigPrefixName() {
    return prefix;
  }

  public String getConfigSourceName() {
    return f_name;
  }

  String getProperty(String key) throws ConfigurationException {
    String e_key = prefix + SEPARATOR + key;
    if (!p.containsKey(e_key)) {
      throw new ConfigurationException(
          String.format(
              "FATAL: Property key %s for mode %s file[%s] not found!", e_key, prefix, f_name));
    }
    return p.getProperty(e_key);
  }

  public static DefaultConfiguration currentSet() {
    return CURRENT;
  }

  // @PipeliteProperty
  public String getPropertyPrefixName() {
    try {
      return getProperty("property.prefix.name");
    } catch (ConfigurationException ce) {
      return null;
    }
  }

  // @PipeliteProperty
  public String getPropertySourceName() {
    return getProperty("property.source.name");
  }

  @PipeliteProperty
  public String getSMTPServer() {
    return getProperty("smtp.server");
  }

  @PipeliteProperty
  public String getDefaultMailTo() {
    return getProperty("default.mail-to");
  }

  @PipeliteProperty
  public int getDefaultLSFCpuCores() {
    return Integer.parseInt(getProperty("default.lsf-cores"));
  }

  @PipeliteProperty
  public String getDefaultLSFQueue() {
    return getProperty("default.lsf-queue");
  }

  @PipeliteProperty
  public int getDefaultLSFMem() {
    return Integer.parseInt(getProperty("default.lsf-mem"));
  }

  @PipeliteProperty
  public int getDefaultLSFMemTimeout() {
    return Integer.parseInt(getProperty("default.lsf-mem-timeout"));
  }

  @PipeliteProperty
  public String getDefaultLSFOutputRedirection() {
    return getProperty("default.lsf-output-redirection");
  }

  @PipeliteProperty
  public String getSMTPMailFrom() {
    return getProperty("smtp.mail-from");
  }

  @PipeliteProperty
  public String getJDBCUser() {
    return getProperty("jdbc.user");
  }

  @PipeliteProperty
  public String getJDBCPassword() {
    return getProperty("jdbc.password");
  }

  @PipeliteProperty
  public String getJDBCDriver() {
    return getProperty("jdbc.driver");
  }

  @PipeliteProperty
  public String getJDBCUrl() {
    return getProperty("jdbc.url");
  }

  public Connection createConnection()
      throws SQLException, ClassNotFoundException {
    Properties props = new Properties();
    props.put("user", getJDBCUser());
    props.put("password", getJDBCPassword());
    props.put("SetBigStringTryClob", "true");

    Class.forName(getJDBCDriver());
    Connection connection =
        new OracleHeartBeatConnection(DriverManager.getConnection(getJDBCUrl(), props));
    connection.setAutoCommit(false);

    return connection;
  }

  @PipeliteProperty
  public String getStageTableName() {
    return getProperty("stage.table.name");
  }

  @PipeliteProperty
  public String getLogTableName() {
    return getProperty("log.table.name");
  }

  @PipeliteProperty
  public String getPipelineName() {
    return getProperty("pipeline.name");
  }

  @PipeliteProperty
  public String getLauncherId() {
    // TODO
    return getProperty("pipeline.launcher.name");
  }

  @PipeliteProperty
  public int getStagesRedoCount() {
    return Integer.parseInt(getProperty("stages.redo.count"));
  }

  @SuppressWarnings("unchecked")
  public <T> T[] loadEnumConstants(String name, Class<?> iface_klass) {
    return (T[]) loadEnum(name, iface_klass).getEnumConstants();
  }

  @SuppressWarnings("unchecked")
  public Class<? extends Enum<?>> loadEnum(String name, Class<?> iface_klass) {
    try {
      Class<? extends Enum<?>> klass = (Class<? extends Enum<?>>) Class.forName(name);

      if (!klass.isEnum()) {
        throw new ConfigurationException("Stages class should be declared as enum");
      }

      Object[] ifaces = klass.getInterfaces();

      lock:
      while (true) {
        for (Object iface : ifaces) {
          if (iface == iface_klass) {
            break lock;
          }
        }
        throw new ConfigurationException(
            "Stages class should implement " + iface_klass.getName() + " interface");
      }

      return klass;

    } catch (ClassNotFoundException cnfe) {
      throw new ConfigurationException(cnfe);
    }
  }

  @PipeliteProperty
  public Stage[] getStages() {
    return loadEnumConstants(getProperty("stages.enum"), Stage.class);
  }

  public Stage getStage(String name) {
    Stage[] stages = getStages();
    for (Stage sd : stages) {
      if (name.equals(sd.toString())) {
        return sd;
      }
    }

    throw new ConfigurationException(
        String.format(
            "No stage with name %s found in enum of %s",
            name, null == stages ? "null" : Arrays.asList(stages)));
  }


  public TaskExecutionResultExceptionResolver getResolver() {
    // TODO: allow the resolver to be changed
    // TODO: optimally the resolver should be for each stage
    return TaskExecutionResultResolver.DEFAULT_EXCEPTION_RESOLVER;
//    return loadEnumConstants(getProperty("commit.status.enum"), ExecutionResult.class);
  }


  @PipeliteProperty
  public String getProcessTableName() {
    return getProperty("process.table.name");
  }

  @PipeliteProperty(required = false)
  public String[] getPropertiesPass() {
    String pass = null;
    try {
      pass = getProperty("properties.pass");
    } catch (ConfigurationException ce) {
      // ignore;
    }

    if (null == pass || 0 == pass.trim().length()) {
      return new String[] {};
    } else {
      return pass.split(":");
    }
  }
}
