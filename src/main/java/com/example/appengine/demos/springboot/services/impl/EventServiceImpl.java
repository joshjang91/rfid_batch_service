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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EventServiceImpl implements EventServiceInterface {

    @Autowired
    public BigQueryDAO bigQueryDAO;
    @Autowired
    public DatastoreDAO datastoreDAO;


    public void processEventData(String lcp)  {
        //read messages from the subscription
        TableResult eventList = bigQueryDAO.getEventData(lcp);
        if (eventList != null && eventList.getTotalRows() > 0) {
            for (List<FieldValue> rowDt : eventList.iterateAll()) {
                if (rowDt.get(0).getValue() != null) {

                    RFIDEvent rfidEvent = null;

                    try {
                        //parse to json
                        rfidEvent = parseRFIDEvent(rowDt);

                        //build event entity with enrichments
                        Entity saveEntity = analyzeEvent(rfidEvent, lcp);
                        //update event_copy: matched & check_count; //FIXME: process this call in a batch manner
                        bigQueryDAO.updateEventToBQ(saveEntity, lcp);
                        //write event to datastore
                        datastoreDAO.writeEventToDS(saveEntity);//FIXME

                    } catch (Exception ex) {
                        // create the error row
                        try {
                            ex.printStackTrace();
                            //LOG.severe(String.format("Got exception processing event.  Exception: %s. Event: %s ", ex.getMessage(), event));//FIXME
                            //writeErrorToBQ(ex.getMessage(), event, lcp);//FIXME
                        } catch (Exception bqex) {
                            bqex.printStackTrace();
                            //LOG.severe(String.format("Got exception writing to BQ Error table.  Exception: %s. Event: %s ", bqex.getMessage(), event));//FIXME
                        }
                    }
                }
            }
        }
    }


    private RFIDEvent parseRFIDEvent(List<FieldValue> eventRow) {
        //TODO build out remaining fields
        if (eventRow != null) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            DateTime eventTime = formatter.parseDateTime("2017-05-08 14:54:46");
//            DateTime eventTime = formatter.parseDateTime(eventRow.get(4).getValue().toString());//FIXME
            return new RFIDEvent(
                    (eventRow.get(1).getValue() != null ? eventRow.get(1).getValue().toString() : null),
                    (eventRow.get(2).getValue() != null ? eventRow.get(2).getValue().toString(): null),
                    (eventRow.get(12).getValue() != null ? eventRow.get(12).getValue().toString(): null),
                    eventTime,//FIXME
                    (eventRow.get(15).getValue() != null ? Integer.parseInt(eventRow.get(15).getValue().toString()): null),
                    (eventRow.get(7).getValue() != null ? eventRow.get(7).getValue().toString(): null),
                    (eventRow.get(8).getValue() != null ? Boolean.parseBoolean(eventRow.get(8).getValue().toString()): null),
                    (eventRow.get(3).getValue() != null ? eventRow.get(3).getValue().toString(): null),
                    (eventRow.get(11).getValue() != null ? eventRow.get(11).getValue().toString(): null),
                    (eventRow.get(6).getValue() != null ? Double.parseDouble(eventRow.get(6).getValue().toString()): null),
                    false,//FIXME
                    (eventRow.get(14).getValue() != null ? Integer.parseInt(eventRow.get(14).getValue().toString()): null),
                    (eventRow.get(13).getValue() != null ? Boolean.parseBoolean(eventRow.get(13).getValue().toString()): null));

        }
        return null;
    }

    private Entity analyzeEvent(RFIDEvent event, String lcp) throws Exception {

        String register = null;
        //DateTime eventTime;

        //FIXME
        //convert event time to proper format for datastore/bq
        //DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        //eventTime = formatter.parseDateTime(event.getEventTime());
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

        //convert ascii tag from hex if necessary
        if (event.getAsciiTag()) {
            event.setTagId(hexToAscii(event.getTagId()));
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


    private String hexToAscii(String hexStr) {
        StringBuilder output = new StringBuilder("");

        for (int i = 0; i < hexStr.length(); i += 2) {
            String str = hexStr.substring(i, i + 2);
            output.append((char) Integer.parseInt(str, 16));
        }
        return output.toString();
    }

}
