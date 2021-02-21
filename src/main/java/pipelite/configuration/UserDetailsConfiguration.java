package pipelite.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
public class UserDetailsConfiguration implements UserDetailsService {

  private final ServiceConfiguration serviceConfiguration;
  private final UserDetails userDetails;

  public UserDetailsConfiguration(@Autowired ServiceConfiguration serviceConfiguration) {
    this.serviceConfiguration = serviceConfiguration;
    PasswordEncoder passwordEncoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
    userDetails =
        User.builder()
            .username(serviceConfiguration.getUsername())
            .password(passwordEncoder.encode(serviceConfiguration.getPassword()))
            .authorities("ADMIN")
            .build();
  }

  @Override
  public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
    if (userDetails.getUsername().equals(username)) {
      return userDetails;
    }
    throw new UsernameNotFoundException("Invalid user name: " + username);
  }
}
