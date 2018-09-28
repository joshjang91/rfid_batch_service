package com.example.appengine.demos.springboot.dao;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import org.springframework.stereotype.Component;

@Component
public class DatastoreDAO {

    /**
     * write entity event to datastore
     *
     * @param event event entity to be written
     * @throws Exception on error
     */
    public void writeEventToDS(Entity event) throws Exception {
        //LOG.info(String.format("Writing event to datastore.  Tagid:   %s ", (String) event.getProperty("tag_id")));//FIXME
        DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
        datastore.put(event);
    }
}
