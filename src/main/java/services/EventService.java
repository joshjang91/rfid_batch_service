package services;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.cloud.bigquery.*;
import com.google.appengine.api.datastore.Entity;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.*;
import dao.RFIDEvent;

public class EventService {

    private static final Map<String, String> BQ_RFID_EVENTS_BY_LCP = new HashMap<String, String>() {
//TODO JOIN TABLES
        {
            //FIXME: improvement to exclude events that occurred in the past 2 hours (SLA)
            put("np", "SELECT video_url, tag_id, reader_id, upc, event_timestamp, curr_ts, " +
                    "product_price, location, exit_event, event_status, product_image_url, " +
                    "product_name, store, matched, check_count, signal FROM `rfid-data-display.rfid_table.event_copy`" +
                    "WHERE check_count < 2 AND matched = false");
            put("pr", "SELECT video_url, tag_id, reader_id, upc, event_timestamp, curr_ts, " +
                    "product_price, location, exit_event, event_status, product_image_url, " +
                    "product_name, store, matched, check_count, signal FROM `rfid-data-display.rfid_table.event_copy`" +
                    "WHERE check_count < 2 AND matched = false");
        }
    };

    private static final Map<String, String> BQ_SALES_BY_LCP = new HashMap<String, String>() {

        {
            put("np", "SELECT STR_NBR, SLS_TS_LOCAL, UPC_CD, SKU_NBR,  UNT_SLS, CURR_RETL_AMT, POS_TRANS_TYP_CD,  "
                    + "POS_TRANS_ID, RGSTR_NBR FROM `rfid-data-display.rfid_table.sales` "
                    + "WHERE STR_NBR = '@storeNumber' AND SLS_TS_LOCAL "
                    + "BETWEEN  '@startTime' AND '@endTime' AND UPC_CD IN ('@upc')");
            put("pr", "SELECT STR_NBR, SLS_TS_LOCAL, UPC_CD, SKU_NBR,  UNT_SLS, CURR_RETL_AMT, POS_TRANS_TYP_CD,  "
                    + "POS_TRANS_ID, RGSTR_NBR FROM `rfid-data-display.rfid_table.sales` "
                    + "WHERE STR_NBR = '@storeNumber' AND SLS_TS_LOCAL "
                    + "BETWEEN  '@startTime' AND '@endTime' AND UPC_CD IN ('@upc')");
        }
    };

    //write event to BQ for analytics purposes
    private static final Map<String, String> BQ_UPDATE_EVENT_LCP = new HashMap<String, String>() {
        {//FIXME CHANGE TO UPDATES
            put("np", "UPDATE `rfid-data-display.rfid_table.event_copy` "
                    + "SET MATCHED =@matched, check_count = @check_count "
                    + "WHERE tag_id = '@tag_id'");
            put("pr", "UPDATE `rfid-data-display.rfid_table.event_copy` "
                    + "SET MATCHED =@matched, check_count = @check_count "
                    + "WHERE tag_id = '@tag_id'");
        }
    };

    //lcp = life cycle process
    public TableResult getEventData(String lcp){
        TableResult result = null;
        try {
            result = EventService.runNamed(BQ_RFID_EVENTS_BY_LCP.get(lcp));
        }catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }


    public static TableResult runNamed(final String queryString)
            throws InterruptedException, FileNotFoundException, IOException {

        //BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        BigQuery bigquery =
                BigQueryOptions.newBuilder().setCredentials(ServiceAccountCredentials.fromStream(
                        new FileInputStream("/Users/iwh0902/projects/secrets/RFID-data-display-mockevent.json"))).build().getService();

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(queryString)
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.
        return queryJob.getQueryResults();

    }

    public Entity analyzeEvent(RFIDEvent event, String lcp) throws Exception {

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

            HashMap<String, Object> salesProps = lookupMatchingSales(event.getStoreNumber(), startTimeNoMillis, endTimeNoMillis, event.getUpc(), lcp);
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

    private RFIDEvent parseRFIDEvent(List<FieldValue> eventRow) {
//TODO build out remaining fields
        if (eventRow != null) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            DateTime eventTime = formatter.parseDateTime("2017-05-08 14:54:46");
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


        public void processEventData(String lcp) throws IOException {
        //read messages from the subscription
        EventService eventService = new EventService();
        TableResult eventList = eventService.getEventData(lcp);
        if (eventList != null && eventList.getTotalRows() > 0) {
            for (List<FieldValue> rowDt : eventList.iterateAll()) {
                if (rowDt.get(0).getValue() != null) {

                    RFIDEvent rfidEvent = null;

                    try {
                        //parse to json
                        rfidEvent = eventService.parseRFIDEvent(rowDt);

                        //build event entity with enrichments
                        Entity saveEntity = eventService.analyzeEvent(rfidEvent, lcp);
                        //update event_copy: matched & check_count; //FIXME: process this call in a batch manner
                        updateEventToBQ(saveEntity, lcp);
                        //write event to datastore
                        //writeEventToDS(saveEntity);//FIXME

                    } catch (Exception ex) {
                        // create the error row
//                        try {
//                            ex.printStackTrace();
//                            LOG.severe(String.format("Got exception processing event.  Exception: %s. Event: %s ", ex.getMessage(), event));
//                            writeErrorToBQ(ex.getMessage(), event, lcp);
//                        } catch (Exception bqex) {
//                            bqex.printStackTrace();
//                            LOG.severe(String.format("Got exception writing to BQ Error table.  Exception: %s. Event: %s ", bqex.getMessage(), event));
//                        }
                    }
                }
            }
        }
    }

    public Entity createEventEntity(String readerId, Date eventTime, Boolean exitReader, String location, String productName,
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


    private HashMap<String, Object> lookupMatchingSales(String storeNumber, String startTime,
                                                              String endTime, String upc, String lcp) throws Exception {

//        LOG.info(String.format("Query sales for store: %s start time: %s end time: %s upd: %s lcp: %s", storeNumber, startTime,
//                endTime, upc, lcp));//FIXME
        HashMap<String, Object> matchedData = new HashMap<String, Object>();
//TODO call sales API
        String queryString = BQ_SALES_BY_LCP.get(lcp).replace("@storeNumber", storeNumber)
                .replace("@startTime", startTime)
                .replace("@endTime", endTime)
                .replace("@upc", upc);

        TableResult result = runNamed(queryString);

        if (result.getTotalRows() > 0) {
            //LOG.info("Found a sale for tag");//FIXME
            for (List<FieldValue> rowDt : result.iterateAll()) {
                if (rowDt.get(0).getValue() != null) {
                    matchedData.put("register", rowDt.get(8).getValue().toString());

                }
            }
        }
        return matchedData;

    }

    /**
     * write entity event to datastore
     *
     * @param event event entity to be written
     * @throws Exception on error
     */
    private static void writeEventToDS(Entity event) throws Exception {
        //LOG.info(String.format("Writing event to datastore.  Tagid:   %s ", (String) event.getProperty("tag_id")));//FIXME
        DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
        datastore.put(event);
    }


    private void updateEventToBQ(Entity event, String lcp) throws Exception {
//TODO write to multiple locations
        //LOG.info(String.format("Writing event to bigquery.  Tagid:   %s ", (String) event.getProperty("tag_id")));//FIXME

        String queryString = BQ_UPDATE_EVENT_LCP.get(lcp)
                .replace("@matched", Boolean.toString((Boolean) event.getProperty("matched")))
                .replace("@check_count", Integer.toString((Integer)event.getProperty("checkedCounter")))
                .replace("@tag_id", (String) event.getProperty("tag_id"));

        // Instantiates a client
        runNamed(queryString);
    }


}
