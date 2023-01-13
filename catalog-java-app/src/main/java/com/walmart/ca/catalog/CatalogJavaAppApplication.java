package com.walmart.ca.catalog;

import com.walmart.ca.catalog.service.ProductIngestionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CatalogJavaAppApplication implements CommandLineRunner {

	@Autowired
	private ProductIngestionService productIngestionService;

	public static void main(String[] args) throws Exception {
		SpringApplication.run(CatalogJavaAppApplication.class, args);
	}

	@Override
	public void run(String...args) throws Exception {
		productIngestionService.initializeProductIngestion();
	}

}
