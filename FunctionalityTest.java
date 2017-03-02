package com.tushar.kafka.test;

import java.util.Arrays;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class FunctionalityTest {

private static void getTwitterData()
{
    final String uri = "http://localhost:9099/getTwitterAnalysis/my-output-topic";
     
    RestTemplate restTemplate = new RestTemplate();
     
    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
    HttpEntity<String> entity = new HttpEntity<String>("parameters", headers);
     
    ResponseEntity<String> result = restTemplate.exchange(uri, HttpMethod.GET, entity, String.class);
     
    System.out.println(result);
}

private void testSpecific(String method){
	switch (method) {
	case "getTwitterData":
		getTwitterData();
		break;

	default:
		break;
	}
}

private void testAll(){
	getTwitterData();
}

public static void main(String[] args) {
	FunctionalityTest test=new FunctionalityTest();
	test.testSpecific("getTwitterData");
	//test.testAll();
}
}
