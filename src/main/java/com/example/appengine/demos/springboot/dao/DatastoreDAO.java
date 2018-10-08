package com.example.appengine.demos.springboot.dao;

import com.example.appengine.demos.springboot.model.RFIDEvent;
import com.google.appengine.api.datastore.*;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.logging.Logger;

@Component
public class DatastoreDAO {

    private static final Logger LOG = Logger.getLogger(RFIDEvent.class.getName());

    /**
     * write entity event to datastore
     *
     * @param event event entity to be written
     * @throws Exception on error
     */
    public void writeEventToDS(Entity event) throws Exception {
        LOG.info(String.format("Writing event to datastore.  Tagid:   %s ", (String) event.getProperty("tag_id")));

        DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();

        Query.Filter propertyFilter =  new Query.FilterPredicate("name_id", Query.FilterOperator.EQUAL, event.getProperty("name_id"));

        Query query = new Query("rfidevent").setFilter(propertyFilter);

        List<Entity> events = datastore.prepare(query).asList(FetchOptions.Builder.withDefaults());

        if(events != null && events.size() > 0) {

            Entity retrievedEvent = events.get(0);
            if(retrievedEvent != null){

                retrievedEvent.setProperty("checkedCounter", event.getProperty("checkedCounter"));
                retrievedEvent.setProperty("matched", event.getProperty("matched"));
            }

            datastore.put(retrievedEvent);

        }else{

            datastore.put(event);

        }
    }

}
