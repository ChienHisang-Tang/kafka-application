/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.jmx;

import java.util.Optional;
import javax.management.ObjectName;
//Exception
import javax.management.MalformedObjectNameException;

/**
 *
 * @author 翔翔
 */
public class IndicatorPool {
    
    private String topicName;
    
    public IndicatorPool(String topicName){
        this.topicName = topicName;
        this.messagesInPerSec = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=" + this.topicName;
        this.endOffsetObjects = "kafka.log:type=Log,name=LogEndOffset,topic=" + this.topicName + ",partition=*";
    }
    
    private Optional<ObjectName> converter(String indicatorStr){
        ObjectName indicator = null;
        try{
            indicator = new ObjectName(indicatorStr);
        }catch(MalformedObjectNameException e){
            e.printStackTrace();
        }
        return Optional.ofNullable(indicator);
    }
    
    //###kafka.server
    private String messagesInPerSec;
    public ObjectName getMessagesInPerSecIndicator(){
        ObjectName out = converter(this.messagesInPerSec).get();       
        return out;
    }

    public ObjectName getMsgInTpsPerSecIndicator(){
        ObjectName out = converter(this.messagesInPerSec).get();
        return out;
    }
    
    private String endOffsetObjects;
    public ObjectName getEndOffsetObjectsIndicator(){
        ObjectName out = converter(this.endOffsetObjects).get();
        return out;
    }
   
    public String forTest(){
        return this.topicName;
    }
}
