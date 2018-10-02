package com.example.appengine.demos.springboot.constants;


public interface EventServiceConstants {


    String BQ_CONVERT_HEX_TO_ASCII = "UPDATE `rfid-data-display.rfid_table.event_copy` event_copy " +
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
                    "WHERE event_copy.ascii_tag_id IS NULL;";

    //FIXME: improvement to exclude events that occurred in the past 2 hours (SLA)
    String BQ_RFID_EVENTS =  "SELECT event.video_url, event.tag_id, reader.reader_id, tag.upc, event.event_timestamp, event.curr_ts, " +
                    "tag.current_retail_amount, reader.location, reader.exit, event.event_status, event.product_image_url, " +
                    "tag.product_description, event.store, event.matched, event.check_count, event.signal, event.name_id " +
                    "FROM `rfid-data-display.rfid_table.event_copy` event " +
                    "LEFT JOIN `rfid-data-display.rfid_table.tag` tag ON tag.tag_id = event.ascii_tag_id " +
                    "LEFT JOIN `rfid-data-display.rfid_table.reader` reader ON reader.reader_id = event.reader_id " +
                    "WHERE check_count < 2 AND matched = false";

    String BQ_SALES =
            "SELECT STR_NBR, SLS_TS_LOCAL, UPC_CD, SKU_NBR,  UNT_SLS, CURR_RETL_AMT, POS_TRANS_TYP_CD,  "
                    + "POS_TRANS_ID, RGSTR_NBR FROM `rfid-data-display.rfid_table.sales` "
                    + "WHERE STR_NBR = '@storeNumber' AND SLS_TS_LOCAL "
                    + "BETWEEN  '@startTime' AND '@endTime' AND UPC_CD IN ('@upc')";

    //write event to BQ for analytics purposes
    String BQ_UPDATE_EVENT =  "UPDATE `rfid-data-display.rfid_table.event_copy` "
                    + "SET MATCHED =@matched, check_count = @check_count "
                    + "WHERE tag_id = '@tag_id'";

    //write error to BigQuery for analytics
    String BQ_ERROR = "INSERT INTO `rfid-data-display.rfid_table.error` "
                    + "(CURR_TS, ERROR, EVENT_DATA) "
                    + "VALUES ('@currTime', '@error', '@event')";

}
