package com.example.appengine.demos.springboot;


import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import services.EventService;


@RestController
public class HelloworldController {
  @GetMapping("/")
  public String hello() {

      try{
          new EventService().processEventData("np");
          return "We made it!";
      }
      catch (Exception e) {
          return "Exception in hello()";
      }


  }
}