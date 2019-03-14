/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.jmx;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.management.MBeanServerConnection;

/**
 *
 * @author 唐健翔
 */
public class JmxManager {
    
    private List<MBeanServerConnection> connectList;
    private List<String> brokerList;
    private boolean isConnect;
    private String port;
    
    public JmxManager(List<String> brokerList , String port){
        connectList = new ArrayList<>();
        this.brokerList = new ArrayList<>();
        this.brokerList.addAll(brokerList);
        this.port = port;
        this.isConnect = false;
    }
    
    public boolean connectJmx(){
        for(String ipTmp : this.brokerList){
            Optional<MBeanServerConnection> conn = JmxConnection.create(ipTmp, this.port);
            if(!conn.isPresent()){
                isConnect = false;
                System.out.println("connect to : " + ipTmp + " STATE : FAIL");
                break;
            }else{
                System.out.println("connect to : " + ipTmp + " STATE : SUCCESS");
                connectList.add(conn.get());
                isConnect = true;
            }
        }
        
        return isConnect;
    }
    
    public long getMsgInCountPerSec(String topic){
        long numInCount = 0;
        if(!this.isConnect)
            return numInCount;
        for(MBeanServerConnection connection : connectList){
            KafkaServerAttribute myAttribute = new KafkaServerAttribute.MsglnCountPerSec(connection, topic);
            numInCount += Long.valueOf(myAttribute.getAttribute());
        }
                
        return numInCount;
    }
    
    
    
}
