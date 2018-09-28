package com.example.appengine.demos.springboot.constants;

import java.util.HashMap;
import java.util.Map;

public interface EventServiceConstants {

    Map<String, String> BQ_RFID_EVENTS_BY_LCP = new HashMap<String, String>() {
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

    Map<String, String> BQ_SALES_BY_LCP = new HashMap<String, String>() {

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
    Map<String, String> BQ_UPDATE_EVENT_LCP = new HashMap<String, String>() {
        {//FIXME CHANGE TO UPDATES
            put("np", "UPDATE `rfid-data-display.rfid_table.event_copy` "
                    + "SET MATCHED =@matched, check_count = @check_count "
                    + "WHERE tag_id = '@tag_id'");
            put("pr", "UPDATE `rfid-data-display.rfid_table.event_copy` "
                    + "SET MATCHED =@matched, check_count = @check_count "
                    + "WHERE tag_id = '@tag_id'");
        }
    };

    String LOCAL_CREDENTIALS_PATH = "/Users/iwh0902/projects/secrets/RFID-data-display-mockevent.json";
}
