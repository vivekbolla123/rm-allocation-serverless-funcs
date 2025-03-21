package com.akasaair.queues;

import java.util.List;

public class DataMismatchedException extends Exception {

    List<?> message;

    public DataMismatchedException(List<?> message){
        this.message=message;
    }
}
