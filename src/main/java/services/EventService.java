package services;

import com.google.cloud.bigquery.*;
import com.google.appengine.api.datastore.Entity;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.*;
import dao.RFIDEvent;

public class EventService {

    private static final Map<String, String> BQ_RFID_EVENTS_BY_LCP = new HashMap<String, String>() {
//TODO JOIN TABLES
        {
            put("np", "SELECT video_url, tag_id, reader_id, upc, event_timestamp, curr_ts, " +
                    "product_price, location, exit_event, event_status, product_image_url, " +
                    "product_name, store, matched, check_count, signal FROM `rfid-data-display.rfid_table.event`" +
                    "WHERE check_count < 2 AND matched = false");
            put("pr", "SELECT video_url, tag_id, reader_id, upc, event_timestamp, curr_ts, " +
                    "product_price, location, exit_event, event_status, product_image_url, " +
                    "product_name, store, matched, check_count, signal FROM `rfid-data-display.rfid_table.event`" +
                    "WHERE check_count < 2 AND matched = false");
        }
    };

    private static final Map<String, String> BQ_SALES_BY_LCP = new HashMap<String, String>() {

        {
            put("np", "SELECT STR_NBR, SLS_TS_LOCAL, DTL.UPC_CD, DTL.SKU_NBR,  DTL.UNT_SLS, DTL.CURR_RETL_AMT, POS_TRANS_TYP_CD,  "
                    + "POS_TRANS_ID, RGSTR_NBR FROM `np-edw-views-thd.SLS.POS_SLS_TRANS_DTL`, UNNEST(DTL) DTL "
                    + "WHERE STR_NBR = '@storeNumber' AND SLS_TS_LOCAL "
                    + "BETWEEN  '@startTime' AND '@endTime' AND DTL.UPC_CD IN ('@upc')");
            put("pr", "SELECT STR_NBR, SLS_TS_LOCAL, DTL.UPC_CD, DTL.SKU_NBR,  DTL.UNT_SLS, DTL.CURR_RETL_AMT, POS_TRANS_TYP_CD,  "
                    + "POS_TRANS_ID, RGSTR_NBR FROM `pr-edw-views-thd.SLS.POS_SLS_TRANS_DTL`, UNNEST(DTL) DTL "
                    + "WHERE STR_NBR = '@storeNumber' AND SLS_TS_LOCAL "
                    + "BETWEEN  '@startTime' AND '@endTime' AND DTL.UPC_CD IN ('@upc')");
        }
    };

    //lcp = life cycle process
    public TableResult getEventData(String lcp){
        StringBuilder returnString = new StringBuilder();//TODO remove?
        TableResult result = null;
        try {
            result = EventService.runNamed(BQ_RFID_EVENTS_BY_LCP.get(lcp));
            if (result.getTotalRows() > 0) {
                for (List<FieldValue> rowDt : result.iterateAll()) {
                    if (rowDt.get(0).getValue() != null) {
                        returnString.append(rowDt.get(0).getValue().toString() + " ");
                        returnString.append(rowDt.get(1).getValue().toString() + " ");
                        returnString.append(rowDt.get(2).getValue().toString() + " ");
                        returnString.append(rowDt.get(3).getValue().toString() + " ");
                        returnString.append(rowDt.get(4).getValue().toString() + " ");
                        returnString.append(rowDt.get(5).getValue().toString() + " ");
                        returnString.append(rowDt.get(6).getValue().toString() + " ");
                        returnString.append(rowDt.get(7).getValue().toString() + " ");
                        returnString.append(rowDt.get(8).getValue().toString() + " ");
                        returnString.append(rowDt.get(9).getValue().toString() + " ");
                        returnString.append(rowDt.get(10).getValue().toString() + " ");
                        returnString.append(rowDt.get(11).getValue().toString() + " ");
                        returnString.append(rowDt.get(12).getValue().toString() + " ");
                        returnString.append(rowDt.get(13).getValue().toString() + " ");
                        returnString.append(rowDt.get(14).getValue().toString() + " ");
                    }
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }


    public static TableResult runNamed(final String queryString)
            throws InterruptedException {

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
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
        DateTime eventTime;

        //convert event time to proper format for datastore/bq
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        eventTime = formatter.parseDateTime(event.getEventTime());
        Date eventTs = new Date(eventTime.getMillis());

        //query sales if exit reader
        if (event.getExitReader()) {
            DateTime endTime = eventTime.plusMinutes(10);
            // *** create the formatter with the "no-millis" format - is there a better way to do this???
            DateTimeFormatter formatterNoMillis = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            String endTimeNoMillis = endTime.toString(formatterNoMillis);
            DateTime startTime = eventTime.minusMinutes(20);
            String startTimeNoMillis = startTime.toString(formatterNoMillis);
            HashMap<String, Object> salesProps = lookupMatchingSales(event.getStoreNumber(), startTimeNoMillis, endTimeNoMillis, event.getUpc(), lcp);
            register = (String) salesProps.get("register");

            //populate the data we got from sales due to a match
            if (register != null) {
                event.setMatched(true);
            }
        }

        if (event.getAsciiTag()) {
            event.setTagId(hexToAscii(event.getTagId()));
        }

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
                event.getAsciiTag(),
                event.getTagId(),
                register,
                event.getCheckedCounter(),
                event.getMatched()
                );
    }



    private RFIDEvent parseRFIDEvent(List<FieldValue> eventRow) {
//TODO build out remaining fields
        if (eventRow != null) {
            return new RFIDEvent(
                    (eventRow.get(1).getValue() != null ? eventRow.get(1).getValue().toString() : null),
                    (eventRow.get(2).getValue() != null ? eventRow.get(2).getValue().toString(): null),
                    (eventRow.get(12).getValue() != null ? eventRow.get(12).getValue().toString(): null),
                    (eventRow.get(4).getValue() != null ? eventRow.get(4).getValue().toString(): null),
                    (eventRow.get(15).getValue() != null ? eventRow.get(15).getValue().toString(): null));

        }
        return null;
    }


    public String processEventData(String lcp) throws IOException {

        StringBuilder testStringToReturn = new StringBuilder();

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

//                        return rfidEvent.toString();//FIXME for testing only

                        //build event entity with enrichments
                        Entity rfidEntity = eventService.analyzeEvent(rfidEvent, lcp);//TODO
                        testStringToReturn.append(rfidEntity.toString());//FIXME for testing only
                        testStringToReturn.append("/n");//FIXME for testing only
                        //write event to datastore
                        //writeEventToDS(rfidEvent);//TODO maybe?
                        //write event to big query
                        //writeEventToBQ(rfidEvent, lcp);//TODO
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
                        return "WE GOT AN EXCEPTION ";//FIXME remove
                    }
                }
            }
        }
        return testStringToReturn.toString();//FIXME for testing only
        //return "end of processEventData";//FIXME remove
    }

    public Entity createEventEntity(String readerId, Date eventTime, Boolean exitReader, String location, String productName,
                                    Double currRetailAmt, String upc, String storeNumber, Boolean asciiTag,
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
}
