package com.example.appengine.demos.springboot.model;


import org.joda.time.DateTime;

/*
convience class for events sent by RFID readers
 */
public class RFIDEvent {

    private String tagId;
    private String receiverId;
    private String storeNumber;
    private DateTime eventTime;
    private Integer signal;
    private String location;
    private Boolean exitReader;
    private String upc;
    private String productName;
    private Double currRetailAmt;
    private Boolean asciiTag;
    private int checkedCounter;
    private Boolean matched;


    public RFIDEvent(String tagId, String receiverId, String storeNumber, DateTime eventTime, Integer signal, String location,
                     Boolean exitReader, String upc, String productName, Double currRetailAmt, Boolean asciiTag, int checkedCounter, Boolean matched) {
        this.tagId = tagId;
        this.receiverId = receiverId;
        this.storeNumber = storeNumber;
        this.eventTime = eventTime;
        this.signal = signal;
        this.location = location;
        this.exitReader = exitReader;
        this.upc = upc;
        this.productName = productName;
        this.currRetailAmt = currRetailAmt;
        this.asciiTag = asciiTag;
        this.checkedCounter = checkedCounter;
        this.matched = matched;

    }

    public Integer getSignal() {
        return signal;
    }

    public void setSignal(Integer signal) {
        this.signal = signal;
    }

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

    public Boolean getAsciiTag() {
        return asciiTag;
    }

    public void setAsciiTag(Boolean asciiTag) {
        this.asciiTag = asciiTag;
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