package cn.doitedu.rtmk.java;

import groovy.lang.GroovyClassLoader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class GroovyTest {
    public static void main(String[] args) throws Exception {

        /*Person person = new Person();
        String s = person.myName();
        System.out.println(s);*/

        /*String code = FileUtils.readFully(new FileReader("d:/code.txt"));
        GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
        Class aClass = groovyClassLoader.parseClass(code);
        GroovyObject o = (GroovyObject) aClass.newInstance();

        Object myStuNo = o.invokeMethod("myStuNo", null);
        System.out.println(myStuNo);*/

        System.out.println("哈哈哈，我是一个java程序");
        System.out.println("哈哈哈，我在干活");

        System.out.println("我要去找一段groovy代码吊着玩");


        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/doit32", "root", "root");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select code from groovy_test");
        rs.next();
        String code = rs.getString("code");

        System.out.println("代码已经被我拿到了");
        System.out.println(code);

        rs.close();
        stmt.close();
        conn.close();

        System.out.println("开始吊着她玩");

        GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
        Class aClass = groovyClassLoader.parseClass(code);
        Human o = (Human) aClass.newInstance();

        String eat = o.whatToEat();
        System.out.println(eat);

    }
}
