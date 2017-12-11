/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.spark;

import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.display.ui.CheckBox;
import org.apache.zeppelin.display.ui.Select;
import org.apache.zeppelin.display.ui.TextBox;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterOutputListener;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResultMessageOutput;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


public class SparkInterpreterTest {

  private SparkInterpreter interpreter;

  // catch the streaming output in onAppend
  private volatile String output = "";
  // catch the interpreter output in onUpdate
  private InterpreterResultMessageOutput messageOutput;


  @Test
  public void testSparkInterpreter() throws IOException, InterruptedException, InterpreterException {
    Properties properties = new Properties();
    properties.setProperty("spark.master", "local");
    properties.setProperty("spark.app.name", "test");
    properties.setProperty("zeppelin.spark.maxResult", "100");
    properties.setProperty("zeppelin.spark.test", "true");
    interpreter = new SparkInterpreter(properties);
    interpreter.setInterpreterGroup(mock(InterpreterGroup.class));
    interpreter.open();

    InterpreterResult result = interpreter.interpret("val a=\"hello world\"", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("a: String = hello world\n", output);

    result = interpreter.interpret("print(a)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("hello world", output);

    // incomplete
    result = interpreter.interpret("println(a", getInterpreterContext());
    assertEquals(InterpreterResult.Code.INCOMPLETE, result.code());

    // syntax error
    result = interpreter.interpret("println(b)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.ERROR, result.code());
    assertTrue(output.contains("not found: value b"));

    // multiple line
    result = interpreter.interpret("\"123\".\ntoInt", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // single line comment
    result = interpreter.interpret("/*comment here*/", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = interpreter.interpret("/*comment here*/\nprint(\"hello world\")", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // multiple line comment
    result = interpreter.interpret("/*line 1 \n line 2*/", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = interpreter.interpret("/*line 1 \n line 2*/print(\"hello world\")", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // companion object
    result = interpreter.interpret("class Counter {\n " +
        "var value: Long = 0} \n" +
        "object Counter {\n def apply(x: Long) = new Counter()\n}", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // spark rdd operation
    result = interpreter.interpret("sc.range(1, 10).sum", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertTrue(output.contains("45"));

    // case class
    result = interpreter.interpret("val bankText = sc.textFile(\"bank.csv\")", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    result = interpreter.interpret(
        "case class Bank(age:Integer, job:String, marital : String, education : String, balance : Integer)\n",
        getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    result = interpreter.interpret(
        "val bank = bankText.map(s=>s.split(\";\")).filter(s => s(0)!=\"\\\"age\\\"\").map(\n" +
            "    s => Bank(s(0).toInt, \n" +
            "            s(1).replaceAll(\"\\\"\", \"\"),\n" +
            "            s(2).replaceAll(\"\\\"\", \"\"),\n" +
            "            s(3).replaceAll(\"\\\"\", \"\"),\n" +
            "            s(5).replaceAll(\"\\\"\", \"\").toInt\n" +
            "        )\n" +
            ")", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // spark version
    result = interpreter.interpret("sc.version", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // spark sql test
    String version = output.trim();
    if (version.contains("String = 1.")) {
      result = interpreter.interpret("sqlContext", getInterpreterContext());
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());

      result = interpreter.interpret(
          "val df = sqlContext.createDataFrame(Seq((1,\"a\"),(2,\"b\")))\n" +
              "df.show()", getInterpreterContext());
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertTrue(output.contains(
              "+---+---+\n" +
              "| _1| _2|\n" +
              "+---+---+\n" +
              "|  1|  a|\n" +
              "|  2|  b|\n" +
              "+---+---+"));
    } else if (version.contains("String = 2.")) {
      result = interpreter.interpret("spark", getInterpreterContext());
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());

      result = interpreter.interpret(
          "val df = spark.createDataFrame(Seq((1,\"a\"),(2,\"b\")))\n" +
              "df.show()", getInterpreterContext());
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertTrue(output.contains(
              "+---+---+\n" +
              "| _1| _2|\n" +
              "+---+---+\n" +
              "|  1|  a|\n" +
              "|  2|  b|\n" +
              "+---+---+"));
    }

    // ZeppelinContext
    result = interpreter.interpret("z.show(df)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.TABLE, messageOutput.getType());
    messageOutput.flush();
    assertEquals("_1\t_2\n1\ta\n2\tb\n", messageOutput.toInterpreterResultMessage().getData());

    InterpreterContext context = getInterpreterContext();
    result = interpreter.interpret("z.input(\"name\", \"default_name\")", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(1, context.getGui().getForms().size());
    assertTrue(context.getGui().getForms().get("name") instanceof TextBox);
    TextBox textBox = (TextBox) context.getGui().getForms().get("name");
    assertEquals("name", textBox.getName());
    assertEquals("default_name", textBox.getDefaultValue());

    context = getInterpreterContext();
    result = interpreter.interpret("z.checkbox(\"checkbox_1\", Seq(\"value_2\"), Seq((\"value_1\", \"name_1\"), (\"value_2\", \"name_2\")))", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(1, context.getGui().getForms().size());
    assertTrue(context.getGui().getForms().get("checkbox_1") instanceof CheckBox);
    CheckBox checkBox = (CheckBox) context.getGui().getForms().get("checkbox_1");
    assertEquals("checkbox_1", checkBox.getName());
    assertEquals(1, checkBox.getDefaultValue().length);
    assertEquals("value_2", checkBox.getDefaultValue()[0]);
    assertEquals(2, checkBox.getOptions().length);
    assertEquals("value_1", checkBox.getOptions()[0].getValue());
    assertEquals("name_1", checkBox.getOptions()[0].getDisplayName());
    assertEquals("value_2", checkBox.getOptions()[1].getValue());
    assertEquals("name_2", checkBox.getOptions()[1].getDisplayName());

    context = getInterpreterContext();
    result = interpreter.interpret("z.select(\"select_1\", Seq(\"value_2\"), Seq((\"value_1\", \"name_1\"), (\"value_2\", \"name_2\")))", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(1, context.getGui().getForms().size());
    assertTrue(context.getGui().getForms().get("select_1") instanceof Select);
    Select select = (Select) context.getGui().getForms().get("select_1");
    assertEquals("select_1", select.getName());
    // TODO(zjffdu) it seems a bug of GUI, the default value should be 'value_2', but it is List(value_2)
    // assertEquals("value_2", select.getDefaultValue());
    assertEquals(2, select.getOptions().length);
    assertEquals("value_1", select.getOptions()[0].getValue());
    assertEquals("name_1", select.getOptions()[0].getDisplayName());
    assertEquals("value_2", select.getOptions()[1].getValue());
    assertEquals("name_2", select.getOptions()[1].getDisplayName());


    // completions
    List<InterpreterCompletion> completions = interpreter.completion("a.", 2, getInterpreterContext());
    assertTrue(completions.size() > 0);

    completions = interpreter.completion("a.isEm", 6, getInterpreterContext());
    assertEquals(1, completions.size());
    assertEquals("isEmpty", completions.get(0).name);

    completions = interpreter.completion("sc.ra", 5, getInterpreterContext());
    assertEquals(1, completions.size());
    assertEquals("range", completions.get(0).name);


    // Zeppelin-Display
    result = interpreter.interpret("import org.apache.zeppelin.display.angular.notebookscope._\n" +
        "import AngularElem._", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = interpreter.interpret("<div style=\"color:blue\">\n" +
        "<h4>Hello Angular Display System</h4>\n" +
        "</div>.display", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.ANGULAR, messageOutput.getType());
    assertTrue(messageOutput.toInterpreterResultMessage().getData().contains("Hello Angular Display System"));

    result = interpreter.interpret("<div class=\"btn btn-success\">\n" +
        "  Click me\n" +
        "</div>.onClick{() =>\n" +
        "  println(\"hello world\")\n" +
        "}.display", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.ANGULAR, messageOutput.getType());
    assertTrue(messageOutput.toInterpreterResultMessage().getData().contains("Click me"));

    // getProgress
    final InterpreterContext context2 = getInterpreterContext();
    Thread interpretThread = new Thread() {
      @Override
      public void run() {
        InterpreterResult result = interpreter.interpret(
            "val df = sc.parallelize(1 to 10, 2).foreach(e=>Thread.sleep(1000))", context2);
        assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      }
    };
    interpretThread.start();
    boolean nonZeroProgress = false;
    int progress = 0;
    while(interpretThread.isAlive()) {
      progress = interpreter.getProgress(context2);
      assertTrue(progress >= 0);
      if (progress != 0 && progress != 100) {
        nonZeroProgress = true;
      }
      Thread.sleep(100);
    }
    assertTrue(nonZeroProgress);

    // cancel
    final InterpreterContext context3 = getInterpreterContext();
    interpretThread = new Thread() {
      @Override
      public void run() {
        InterpreterResult result = interpreter.interpret(
            "val df = sc.parallelize(1 to 10, 2).foreach(e=>Thread.sleep(1000))", context3);
        assertEquals(InterpreterResult.Code.ERROR, result.code());
        assertTrue(output.contains("cancelled"));
      }
    };

    interpretThread.start();
    // sleep 1 second to wait for the spark job start
    Thread.sleep(1000);
    interpreter.cancel(context3);
    interpretThread.join();
  }

  @Test
  public void testDependencies() throws IOException, InterpreterException {
    Properties properties = new Properties();
    properties.setProperty("spark.master", "local");
    properties.setProperty("spark.app.name", "test");
    properties.setProperty("zeppelin.spark.maxResult", "100");
    // download spark-avro jar
    URL website = new URL("http://repo1.maven.org/maven2/com/databricks/spark-avro_2.11/3.2.0/spark-avro_2.11-3.2.0.jar");
    ReadableByteChannel rbc = Channels.newChannel(website.openStream());
    File avroJarFile = new File("spark-avro_2.11-3.2.0.jar");
    FileOutputStream fos = new FileOutputStream(avroJarFile);
    fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

    properties.setProperty("spark.jars", avroJarFile.getAbsolutePath());

    interpreter = new SparkInterpreter(properties);
    interpreter.setInterpreterGroup(mock(InterpreterGroup.class));
    interpreter.open();

    InterpreterResult result = interpreter.interpret("import com.databricks.spark.avro._", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
  }


  @After
  public void tearDown() {
    if (this.interpreter != null) {
      this.interpreter.close();
    }
  }

  private InterpreterContext getInterpreterContext() {
    output = "";
    return new InterpreterContext(
        "noteId",
        "paragraphId",
        "replName",
        "paragraphTitle",
        "paragraphText",
        new AuthenticationInfo(),
        new HashMap<String, Object>(),
        new GUI(),
        new AngularObjectRegistry("spark", null),
        null,
        null,
        new InterpreterOutput(

            new InterpreterOutputListener() {
              @Override
              public void onUpdateAll(InterpreterOutput out) {

              }

              @Override
              public void onAppend(int index, InterpreterResultMessageOutput out, byte[] line) {
                try {
                  output = out.toInterpreterResultMessage().getData();
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }

              @Override
              public void onUpdate(int index, InterpreterResultMessageOutput out) {
                messageOutput = out;
              }
            })
    );
  }
}
