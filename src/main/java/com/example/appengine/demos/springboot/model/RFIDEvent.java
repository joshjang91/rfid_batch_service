package com.example.appengine.demos.springboot.model;


import org.joda.time.DateTime;

/*
convience class for events sent by RFID readers
 */
public class RFIDEvent {

    private String uniqueEventId;
    private String tagId;
    private String receiverId;
    private String storeNumber;
    private DateTime eventTime;
    private String location;
    private Boolean exitReader;
    private String upc;
    private String productName;
    private Double currRetailAmt;
    private int checkedCounter;
    private Boolean matched;


    public RFIDEvent(String uniqueEventId, String tagId, String receiverId, String storeNumber, DateTime eventTime, String location,
                     Boolean exitReader, String upc, String productName, Double currRetailAmt, int checkedCounter, Boolean matched) {
        this.uniqueEventId = uniqueEventId;
        this.tagId = tagId;
        this.receiverId = receiverId;
        this.storeNumber = storeNumber;
        this.eventTime = eventTime;
        this.location = location;
        this.exitReader = exitReader;
        this.upc = upc;
        this.productName = productName;
        this.currRetailAmt = currRetailAmt;
        this.checkedCounter = checkedCounter;
        this.matched = matched;

    }

    public String getUniqueEventId() { return uniqueEventId; }

    public String getTagId() {
        return tagId;
    }

    public void setTagId(String tagId) {
        this.tagId = tagId;
    }

    public DateTime getEventTime() {
        return eventTime;
    }

    public void setEventTime(DateTime eventTime) {
        this.eventTime = eventTime;
    }

    public String getStoreNumber() {
        return storeNumber;
    }

    public void setStoreNumber(String storeNumber) {
        this.storeNumber = storeNumber;
    }

    public String getReceiverId() {
        return receiverId;
    }

    public void setReceiverId(String receiverId) {
        this.receiverId = receiverId;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Boolean getExitReader() {
        return exitReader;
    }

    public void setExitReader(Boolean exitReader) {
        this.exitReader = exitReader;
    }

    public String getUpc() {
        return upc;
    }

    public void setUpc(String upc) {
        this.upc = upc;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Double getCurrRetailAmt() {
        return currRetailAmt;
    }

    public void setCurrRetailAmt(Double currRetailAmt) {
        this.currRetailAmt = currRetailAmt;
    }

    public String toString()
    {
        return (String.format(
                "Store:  %s, Reader Id:  %s, Tag id: %s, Event Time: %s, UPC: %s, Matched: %B",
                getStoreNumber(),  getReceiverId(), getTagId(), getEventTime().toString(), getUpc(), getMatched()));

    }

    public int getCheckedCounter() {
        return checkedCounter;
    }

    public void setCheckedCounter(int checkedCounter) {
        this.checkedCounter = checkedCounter;
    }

    public Boolean getMatched() {
        return matched;
    }

    public void setMatched(Boolean matched) {
        this.matched = matched;
    }
}