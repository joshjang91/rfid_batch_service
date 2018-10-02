package com.example.appengine.demos.springboot.constants;

import java.util.HashMap;
import java.util.Map;

public interface EventServiceConstants {


    Map<String, String> BQ_CONVERT_HEX_TO_ASCII_BY_LCP = new HashMap<String, String>() {
        {
            put("np", "UPDATE `rfid-data-display.rfid_table.event_copy` event_copy " +
                    "SET ascii_tag_id = " +
                    "  (SELECT " +
                    "    (CASE " +
                    "      WHEN tag.tag_id IS NULL AND REGEXP_CONTAINS(event.tag_id, r'^[0-9a-fA-F]+$') THEN CAST(FROM_HEX(event.tag_id) AS STRING) " +
                    "      ELSE event.tag_id " +
                    "    END) AS ascii_tag_id " +
                    "    FROM `rfid-data-display.rfid_table.event_copy` event " +
                    "    LEFT JOIN `rfid-data-display.rfid_table.tag` tag ON tag.tag_id = event.tag_id " +
                    "    WHERE event_copy.name_id = event.name_id " +
                    "   ) " +
                    "WHERE event_copy.ascii_tag_id IS NULL;");
            put("pr",  "UPDATE `rfid-data-display.rfid_table.event_copy` event_copy " +
                    "SET ascii_tag_id = " +
                    "  (SELECT " +
                    "    (CASE " +
                    "      WHEN tag.tag_id IS NULL AND REGEXP_CONTAINS(event.tag_id, r'^[0-9a-fA-F]+$') THEN CAST(FROM_HEX(event.tag_id) AS STRING) " +
                    "      ELSE event.tag_id " +
                    "    END) AS ascii_tag_id " +
                    "    FROM `rfid-data-display.rfid_table.event_copy` event " +
                    "    LEFT JOIN `rfid-data-display.rfid_table.tag` tag ON tag.tag_id = event.tag_id " +
                    "    WHERE event_copy.name_id = event.name_id " +
                    "   ) " +
                    "WHERE event_copy.ascii_tag_id IS NULL;");

        }
    };

    Map<String, String> BQ_RFID_EVENTS_BY_LCP = new HashMap<String, String>() {
        {
            //FIXME: improvement to exclude events that occurred in the past 2 hours (SLA)
            put("np", "SELECT event.video_url, event.tag_id, reader.reader_id, tag.upc, event.event_timestamp, event.curr_ts, " +
                    "tag.current_retail_amount, reader.location, reader.exit, event.event_status, event.product_image_url, " +
                    "tag.product_description, event.store, event.matched, event.check_count, event.signal " +
                    "FROM `rfid-data-display.rfid_table.event_copy` event " +
                    "LEFT JOIN `rfid-data-display.rfid_table.tag` tag ON tag.tag_id = event.ascii_tag_id " +
                    "LEFT JOIN `rfid-data-display.rfid_table.reader` reader ON reader.reader_id = event.reader_id " +
                    "WHERE check_count < 2 AND matched = false");
            put("pr", "SELECT event.video_url, event.tag_id, reader.reader_id, tag.upc, event_timestamp, event.curr_ts, " +
                    "tag.current_retail_amount, reader.location, reader.exit, event.event_status, event.product_image_url, " +
                    "tag.product_description, event.store, event.matched, event.check_count, event.signal " +
                    "FROM `rfid-data-display.rfid_table.event_copy` event " +
                    "LEFT JOIN `rfid-data-display.rfid_table.tag` tag ON tag.tag_id = event.ascii_tag_id " +
                    "LEFT JOIN `rfid-data-display.rfid_table.reader` reader ON reader.reader_id = event.reader_id " +
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
        {
            put("np", "UPDATE `rfid-data-display.rfid_table.event_copy` "
                    + "SET MATCHED =@matched, check_count = @check_count "
                    + "WHERE tag_id = '@tag_id'");
            put("pr", "UPDATE `rfid-data-display.rfid_table.event_copy` "
                    + "SET MATCHED =@matched, check_count = @check_count "
                    + "WHERE tag_id = '@tag_id'");
        }
    };

    //write error to BigQuery for analytics
    Map<String, String> BQ_ERROR_LCP = new HashMap<String, String>() {
        {
            put("np", "INSERT INTO `rfid-data-display.rfid_table.error` "
                    + "(CURR_TS, ERROR, EVENT_DATA) "
                    + "VALUES ('@currTime', '@error', '@event')");
            put("pr", "INSERT INTO `rfid-data-display.rfid_table.error` "
                    + "(CURR_TS, ERROR, EVENT_DATA) "
                    + "VALUES ('@currTime', '@error', '@event')");
        }
    };

    String LOCAL_CREDENTIALS_PATH = "/Users/iwh0902/projects/secrets/RFID-data-display-mockevent.json";
}
