package com.example.appengine.demos.springboot.dao;

import com.google.appengine.api.datastore.Entity;
import com.google.cloud.bigquery.*;
import com.example.appengine.demos.springboot.constants.EventServiceConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;

@Component
public class BigQueryDAO {

    @Autowired
    BigQueryUtilities bigQueryUtilities;

    public BigQueryDAO (BigQueryUtilities bigQueryUtilities) {
        this.bigQueryUtilities = bigQueryUtilities;
    }

    public TableResult getEventData(String lcp){
        TableResult result = null;
        try {
            result = bigQueryUtilities.runNamed(EventServiceConstants.BQ_RFID_EVENTS_BY_LCP.get(lcp));
        }catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public HashMap<String, Object> lookupMatchingSales(String storeNumber, String startTime,
                                                        String endTime, String upc, String lcp) throws Exception {

//        LOG.info(String.format("Query sales for store: %s start time: %s end time: %s upd: %s lcp: %s", storeNumber, startTime, endTime, upc, lcp));//FIXME
        HashMap<String, Object> matchedData = new HashMap<String, Object>();
        //TODO call sales API
        String queryString = EventServiceConstants.BQ_SALES_BY_LCP.get(lcp).replace("@storeNumber", storeNumber)
                .replace("@startTime", startTime)
                .replace("@endTime", endTime)
                .replace("@upc", upc);

        TableResult result = bigQueryUtilities.runNamed(queryString);

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

    public void updateEventToBQ(Entity event, String lcp) throws Exception {
        //TODO write to multiple locations
        //LOG.info(String.format("Writing event to bigquery.  Tagid:   %s ", (String) event.getProperty("tag_id")));//FIXME

        String queryString = EventServiceConstants.BQ_UPDATE_EVENT_LCP.get(lcp)
                .replace("@matched", Boolean.toString((Boolean) event.getProperty("matched")))
                .replace("@check_count", Integer.toString((Integer)event.getProperty("checkedCounter")))
                .replace("@tag_id", (String) event.getProperty("tag_id"));

        // Instantiates a client
        bigQueryUtilities.runNamed(queryString);
    }
}
