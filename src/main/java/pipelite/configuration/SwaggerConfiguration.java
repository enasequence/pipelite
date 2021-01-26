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

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfiguration {

  @Autowired
  ServiceConfiguration serviceConfiguration;

  @Bean
  public OpenAPI openAPI() {
    return new OpenAPI()
        .addServersItem(new Server().url(serviceConfiguration.getContextPath()))
        .components(
            new Components()
                .addSecuritySchemes(
                    "basicScheme",
                    new SecurityScheme().type(SecurityScheme.Type.HTTP).scheme("basic")))
        .addSecurityItem(new SecurityRequirement().addList("basicScheme"))
        .info(new Info().version("1").title("Pipelite launcher and scheduler services"));
  }
}
