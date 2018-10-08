package com.example.appengine.demos.springboot;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import com.example.appengine.demos.springboot.services.impl.EventServiceImpl;


@RestController
public class HelloworldController {

    @Autowired
    public EventServiceImpl eventService;

      @GetMapping("/")
      public String hello() {


      try{
          eventService.processEventData();
          return "We made it!\n";
      }
      catch (Exception e) {
          e.printStackTrace();
          return "Exception in hello()";
      }


  }
}