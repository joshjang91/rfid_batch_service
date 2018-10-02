package com.example.appengine.demos.springboot.dao;

import com.example.appengine.demos.springboot.model.RFIDEvent;
import com.google.appengine.api.datastore.Entity;
import com.google.cloud.bigquery.*;
import com.example.appengine.demos.springboot.constants.EventServiceConstants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

@Component
public class BigQueryDAO {

    @Autowired
    BigQueryUtilities bigQueryUtilities;

    private static final Logger LOG = Logger.getLogger(RFIDEvent.class.getName());

    public BigQueryDAO (BigQueryUtilities bigQueryUtilities) {
        this.bigQueryUtilities = bigQueryUtilities;
    }

    public TableResult getEventData(){
        TableResult result = null;
        try {
            result = bigQueryUtilities.runNamed(EventServiceConstants.BQ_RFID_EVENTS);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public HashMap<String, Object> lookupMatchingSales(String storeNumber, String startTime,
                                                        String endTime, String upc) throws Exception {

        LOG.info(String.format("Query sales for store: %s start time: %s end time: %s upd: %s", storeNumber, startTime, endTime, upc));
        HashMap<String, Object> matchedData = new HashMap<String, Object>();

        String queryString = EventServiceConstants.BQ_SALES.replace("@storeNumber", storeNumber)
                .replace("@startTime", startTime)
                .replace("@endTime", endTime)
                .replace("@upc", upc);

        TableResult result = bigQueryUtilities.runNamed(queryString);

        if (result.getTotalRows() > 0) {
            LOG.info("Found a sale for tag");
            for (List<FieldValue> rowDt : result.iterateAll()) {
                if (rowDt.get(0).getValue() != null) {
                    matchedData.put("register", rowDt.get(8).getValue().toString());

                }
            }
        }
        return matchedData;
    }

    public void updateEventToBQ(Entity event) throws Exception {
        LOG.info(String.format("Writing event to bigquery.  Tagid:   %s ", (String) event.getProperty("tag_id")));

        String queryString = EventServiceConstants.BQ_UPDATE_EVENT
                .replace("@matched", Boolean.toString((Boolean) event.getProperty("matched")))
                .replace("@check_count", Integer.toString((Integer)event.getProperty("checkedCounter")))
                .replace("@tag_id", (String) event.getProperty("tag_id"));

        // Instantiates a client
        bigQueryUtilities.runNamed(queryString);
    }

    /**
     * write error event to big query
     *
     * @param error error which occurred
     * @param event event entity to be written
     * @throws Exception on error
     */
    public void writeErrorToBQ(String error, RFIDEvent event) throws Exception {
        String queryString = EventServiceConstants.BQ_ERROR.replace("@currTime",
                ISODateTimeFormat.dateTime().print(new DateTime(DateTimeZone.UTC)))
                .replace("@error", error)
                .replace("@event", event.toString());

        // Instantiates a client
        bigQueryUtilities.runNamed(queryString);
    }

    public TableResult convertHexToAscii(){
        TableResult result = null;
        try {
            result = bigQueryUtilities.runNamed(EventServiceConstants.BQ_CONVERT_HEX_TO_ASCII);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return result;

    }

}
