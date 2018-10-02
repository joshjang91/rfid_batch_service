package com.example.appengine.demos.springboot.services.impl;

import com.example.appengine.demos.springboot.services.EventServiceInterface;
import com.google.cloud.bigquery.*;
import com.google.appengine.api.datastore.Entity;
import com.example.appengine.demos.springboot.dao.BigQueryDAO;
import com.example.appengine.demos.springboot.dao.DatastoreDAO;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;
import com.example.appengine.demos.springboot.model.RFIDEvent;
import java.util.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EventServiceImpl implements EventServiceInterface {

    @Autowired
    public BigQueryDAO bigQueryDAO;
    @Autowired
    public DatastoreDAO datastoreDAO;

    private static final Logger LOG = Logger.getLogger(RFIDEvent.class.getName());

    public void processEventData(String lcp)  {
        //convert any existing hex tag values to ascii
        //TODO function to convert tags from hex to ascii
        bigQueryDAO.convertHexToAscii(lcp);
        //read messages from the subscription
        TableResult eventList = bigQueryDAO.getEventData(lcp);
        if (eventList != null && eventList.getTotalRows() > 0) {
            for (List<FieldValue> rowDt : eventList.iterateAll()) {
                if (rowDt.get(0).getValue() != null) {

                    RFIDEvent rfidEvent = null;

                    try {
                        //parse to json
                        rfidEvent = parseRFIDEvent(rowDt);

                        if(rfidEvent.getUpc() == null){
                            throw new Exception(String.format("no matching tag data found for tag id: %s  store: %s ", rfidEvent.getTagId(), rfidEvent.getStoreNumber()));
                        }
                        if(rfidEvent.getReceiverId() == null){
                            throw new Exception(String.format("no matching readers found for tag id: %s  store: %s ", rfidEvent.getTagId(), rfidEvent.getStoreNumber()));
                        }

                        //build event entity with enrichments
                        Entity saveEntity = analyzeEvent(rfidEvent, lcp);
                        //update event_copy: matched & check_count;
                        bigQueryDAO.updateEventToBQ(saveEntity, lcp);
                        //write event to datastore
                        datastoreDAO.writeEventToDS(saveEntity);

                    } catch (Exception ex) {
                        // create the error row
                        try {
                            ex.printStackTrace();
                            LOG.severe(String.format("Got exception processing event.  Exception: %s. Event: %s ", ex.getMessage(), rfidEvent));
                            bigQueryDAO.writeErrorToBQ(ex.getMessage(), rfidEvent, lcp);
                        } catch (Exception bqex) {
                            bqex.printStackTrace();
                            LOG.severe(String.format("Got exception writing to BQ Error table.  Exception: %s. Event: %s ", bqex.getMessage(), rfidEvent));
                        }
                    }
                }
            }
        }
    }


    private RFIDEvent parseRFIDEvent(List<FieldValue> eventRow) {
        if (eventRow != null) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss z");
            DateTime eventTime = formatter.parseDateTime(eventRow.get(4).getValue().toString());
            return new RFIDEvent(
                    (eventRow.get(1).getValue() != null ? eventRow.get(1).getValue().toString() : null),
                    (eventRow.get(2).getValue() != null ? eventRow.get(2).getValue().toString(): null),
                    (eventRow.get(12).getValue() != null ? eventRow.get(12).getValue().toString(): null),
                    eventTime,
                    (eventRow.get(15).getValue() != null ? Integer.parseInt(eventRow.get(15).getValue().toString()): null),
                    (eventRow.get(7).getValue() != null ? eventRow.get(7).getValue().toString(): null),
                    (eventRow.get(8).getValue() != null ? Boolean.parseBoolean(eventRow.get(8).getValue().toString()): null),
                    (eventRow.get(3).getValue() != null ? eventRow.get(3).getValue().toString(): null),
                    (eventRow.get(11).getValue() != null ? eventRow.get(11).getValue().toString(): null),
                    (eventRow.get(6).getValue() != null ? Double.parseDouble(eventRow.get(6).getValue().toString()): null),
                    (eventRow.get(14).getValue() != null ? Integer.parseInt(eventRow.get(14).getValue().toString()): null),
                    (eventRow.get(13).getValue() != null ? Boolean.parseBoolean(eventRow.get(13).getValue().toString()): null));

        }
        return null;
    }

    private Entity analyzeEvent(RFIDEvent event, String lcp) throws Exception {

        String register = null;

        Date eventTs = new Date(event.getEventTime().getMillis());

        //query sales if exit reader
        if (event.getExitReader()) {
            DateTime endTime = event.getEventTime().plusMinutes(10);
            // *** create the formatter with the "no-millis" format - is there a better way to do this???
            DateTimeFormatter formatterNoMillis = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            String endTimeNoMillis = endTime.toString(formatterNoMillis);
            DateTime startTime = event.getEventTime().minusMinutes(20);
            String startTimeNoMillis = startTime.toString(formatterNoMillis);

            HashMap<String, Object> salesProps = bigQueryDAO.lookupMatchingSales(event.getStoreNumber(), startTimeNoMillis, endTimeNoMillis, event.getUpc(), lcp);
            register = (String) salesProps.get("register");

            //populate the data we got from sales due to a match
            if (register != null) {
                event.setMatched(true);
            }
        }

        //increment check counter - regardless of match
        event.setCheckedCounter(event.getCheckedCounter()+1);

        return createEventEntity(
                event.getReceiverId(),
                eventTs,
                event.getExitReader(),
                event.getLocation(),
                event.getProductName(),
                event.getCurrRetailAmt(),
                event.getUpc(),
                event.getStoreNumber(),
                event.getTagId(),
                register,
                event.getCheckedCounter(),
                event.getMatched()
                );
    }


    private Entity createEventEntity(String readerId, Date eventTime, Boolean exitReader, String location, String productName,
                                    Double currRetailAmt, String upc, String storeNumber,
                                    String tagId, String register, int checkedCounter, Boolean matched) {
        Entity eventEntity = new Entity("event", UUID.randomUUID().toString());
        //reader info
        eventEntity.setProperty("curr_ts", new Date());
        eventEntity.setProperty("reader_id", readerId);
        eventEntity.setProperty("event_status", "new");
        eventEntity.setProperty("event_timestamp", eventTime);
        eventEntity.setProperty("exit_event", exitReader);
        eventEntity.setProperty("location", location);
        eventEntity.setProperty("product_image_url", "");
        eventEntity.setProperty("product_name", productName);
        eventEntity.setProperty("product_price", currRetailAmt);
        eventEntity.setProperty("upc", upc);
        eventEntity.setProperty("store", storeNumber);
        eventEntity.setProperty("tag_id", tagId);
        eventEntity.setProperty("video_url", "");
        eventEntity.setProperty("matched", matched);
        eventEntity.setProperty("checkedCounter", checkedCounter);
        if(register != null){
            eventEntity.setProperty("register", register);
        }else{
            eventEntity.setProperty("register", "");
        }

        return eventEntity;
    }

}
