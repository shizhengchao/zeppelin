package org.apache.zeppelin.integration;

import kong.unirest.json.JSONObject;
import org.apache.zeppelin.client.ClientConfig;
import org.apache.zeppelin.client.ExecuteResult;
import org.apache.zeppelin.client.ZSession;

import java.util.HashMap;
import java.util.Map;

public class Test {

  public static void main(String[] args) throws Exception {
    ClientConfig clientConfig = new ClientConfig("http://localhost:28086");

    Map<String, String> intpProperties = new HashMap<>();

    ZSession session = ZSession.builder()
            .setClientConfig(clientConfig)
            .setInterpreter("hive")
            .setIntpProperties(intpProperties)
            .build();

    try {
      session.start();
      // show databases
      ExecuteResult result = session.submit("show tables");
      result = session.waitUntilFinished(result.getStatementId());
      System.out.println(result.toString());

      // select statement
      result = session.submit("SELECT count(1) from bank");
      result = session.waitUntilFinished(result.getStatementId());
      System.out.println(result.toString());

      // select statement
      Map<String,String> localProperties = new HashMap<>();
      localProperties.put("user", "hadoop");
      result = session.submit("", localProperties, "SELECT count(1) from bank");
      result = session.waitUntilFinished(result.getStatementId());
      System.out.println(result.toString());

    } finally {
      session.stop();
    }
  }
}
