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
package pipelite.launcher;

import static org.springframework.test.annotation.DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PipeliteTestConfiguration.class,
    properties = {"pipelite.launcher.processRunnerFrequency=250ms"})
@ContextConfiguration(initializers = PipeliteSchedulerHSqlTest.TestContextInitializer.class)
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
@DirtiesContext(classMode = BEFORE_EACH_TEST_METHOD)
@Tag("hsql-test")
public class PipeliteSchedulerHSqlTest {

  @Autowired PipeliteSchedulerTester tester;

  public static class TestContextInitializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
      TestPropertyValues.of(
              "pipelite.launcher.schedulerName=" + UniqueStringGenerator.randomSchedulerName())
          .applyTo(configurableApplicationContext.getEnvironment());
    }
  }

  @Test
  public void testTwoSuccessSchedules() {
    tester.testTwoSuccessSchedules();
  }

  @Test
  public void testTwoFailureSchedules() {
    tester.testTwoFailureSchedules();
  }

  @Test
  public void testTwoExceptionSchedules() {
    tester.testTwoExceptionSchedules();
  }

  @Test
  public void testTwoSuccessTwoFailureTwoExceptionSchedule() {
    tester.testTwoSuccessTwoFailureTwoExceptionSchedule();
  }
}
