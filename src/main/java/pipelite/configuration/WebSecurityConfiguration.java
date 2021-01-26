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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

@EnableWebSecurity
public class WebSecurityConfiguration extends WebSecurityConfigurerAdapter {

  public static final String HEALTH_ENDPOINT = "/actuator/health";

  @Autowired
  ServiceConfiguration serviceConfiguration;

  @Bean
  public UserDetailsService userDetailsService() {
    User.UserBuilder users = User.withDefaultPasswordEncoder();
    InMemoryUserDetailsManager manager = new InMemoryUserDetailsManager();
    manager.createUser(
        users
            .username(serviceConfiguration.getUsername())
            .password(serviceConfiguration.getPassword())
            .roles("ADMIN")
            .build());
    return manager;
  }

  @Override
  public void configure(WebSecurity web) {
    web.ignoring().antMatchers("/static/**").and().ignoring().antMatchers(HEALTH_ENDPOINT);
  }

  @Override
  protected void configure(HttpSecurity httpSecurity) throws Exception {
    httpSecurity
        .csrf()
        .disable()
        .cors()
        .and()
        .authorizeRequests()
        .antMatchers("static/**")
        .permitAll()
        .antMatchers(HEALTH_ENDPOINT)
        .permitAll()
        .anyRequest()
        .authenticated()
        .and()
        .formLogin()
        .defaultSuccessUrl("/ui/processes", true)
        .loginPage("/login")
        .permitAll()
        .and()
        .logout()
        .permitAll();
    //  .antMatchers("/*").permitAll();
    httpSecurity.csrf().disable();
  }

  @Bean
  CorsConfigurationSource corsConfigurationSource() {
    CorsConfiguration configuration = new CorsConfiguration();
    configuration.setAllowCredentials(true);
    configuration.addAllowedOrigin("*");
    configuration.addAllowedHeader("*");
    configuration.addAllowedMethod("*");
    UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", configuration);
    return source;
  }
}
