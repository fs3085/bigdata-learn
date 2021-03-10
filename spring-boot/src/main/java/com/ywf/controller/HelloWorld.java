package com.ywf.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class HelloWorld {

    @RequestMapping("/")
    public String HelloWorld(){
        return "hello world";
    }

}
