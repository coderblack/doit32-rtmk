package cn.doitedu.rtmk.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class HelloController {

    @RequestMapping("/add")
    public Integer add(int a,int b){
        return a+b;
    }

    @RequestMapping("/minus")
    public Integer minus(int a,int b){
        return a-b;
    }

}
