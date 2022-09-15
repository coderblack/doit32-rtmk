package cn.doitedu.rtmk.realtimemarketingmanager.controller;

import cn.doitedu.rtmk.realtimemarketingmanager.pojo.Person;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@RestController
public class HiController {
    Random random = new Random();
    String[] genders = {"男","女"};

    @RequestMapping("/hello")
    public String hello(String name){


        return "hello " + name;
    }


    @RequestMapping("/hello2")
    public Person hello2(String name){
        return new Person(name,genders[random.nextInt(2)], random.nextInt(100));
    }

}
