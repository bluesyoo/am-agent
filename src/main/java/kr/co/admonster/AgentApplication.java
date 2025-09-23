package kr.co.admonster;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
public class AgentApplication {
	
	public static void main(String[] args) {
		SpringApplication.run(AgentApplication.class, args);
	}
	
}
