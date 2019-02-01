/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.jmx;


import com.dslab.kafka.app.TopicList.Topic;
//java lib
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Optional;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
//exception
import java.io.IOException;
import java.net.MalformedURLException;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

/**
 *
 * @author 
 */
public class JmxConnection {
    private static Logger log = LoggerFactory.getLogger(JmxConnection.class);
    private static MBeanServerConnection conn;
    
    
    public static Optional<MBeanServerConnection> create(String ip , String port){
        String jmxURL = "service:jmx:rmi:///jndi/rmi://" + ip + ":" + port + "/jmxrmi";
        log.info( "init jmx, jmxUrl: {}, and begin to connect it" , jmxURL);
        try{
            JMXServiceURL serviceURL = new JMXServiceURL(jmxURL);
            JMXConnector connector = JMXConnectorFactory.connect(serviceURL,null);
            conn = connector.getMBeanServerConnection();
            if(conn != null)
                return Optional.of(conn);
            else
                return Optional.empty();
        }catch(MalformedURLException e){
            e.printStackTrace();
            return Optional.empty();
        }catch(IOException e){
            e.printStackTrace();
            return Optional.empty();
        }
        
    }
    
    //Optional<一定要是物件>
 /*   public long getMsglnCountPerSec(Topic topicName){
        Optional<Object> opt = getAttributeWorker(topicName.toString() , "Count");
        return (long)opt.get();
    }
    
    public double getMsgInTpsPerSec(Topic topicName){
        Optional<Object> opt = getAttributeWorker(topicName.toString() , "OneMinuteRate");
        return (double)opt.get();
    }
    
    public Map<Integer,Long> getTopicEndOffset(Topic topicName){
        ObjectName objName = null;
        Set<ObjectName> objNames = null;
        Map<Integer , Long> map = new HashMap<>();
               
        try{
            objName = topicNameConverter(topicName.toString() , "Value").get();
            objNames = conn.queryNames(objName, null);
        }catch(Exception e){
            e.printStackTrace();
        }
        
        if(objNames == null)
            return (Map<Integer,Long>)Optional.empty().get();
        for(ObjectName objname : objNames){
            int partId = Integer.parseInt(objname.getKeyProperty("partition"));
            Optional opt = getAttribute(objname , "Value");
            if(opt.isPresent()){
                Object obj = opt.get();
                long l = Long.valueOf(String.valueOf(obj));
                map.put(partId , l);
            }
        }
        return map;
        
    }
       
    private Optional<ObjectName> topicNameConverter(String topicName , String option){      
        ObjectName name = null;
        String out;
        switch(option){
            case "Count":
                out = new IndicatorPool(topicName).getMessagesInPerSecIndicator();break;
            case "OneMinuteRate":
                out = new IndicatorPool(topicName).getMsgInTpsPerSecIndicator();break;
            case "Value":
                out = new IndicatorPool(topicName).getEndOffsetObjectsIndicator();break;
            default:
                return Optional.empty();
        }
        
        try{
            name = new ObjectName(out);
        }catch(MalformedObjectNameException e){
            e.printStackTrace();
        }
        return Optional.ofNullable(name);
    }
    
    private Optional<Object> getAttributeWorker(String topicName , String option){
        Object value = null;      
        if(conn == null){
            log.error("jmx connection is null");
            return Optional.empty();
        }
        
        try{
            Optional<ObjectName> opt = topicNameConverter(topicName , option);
            value = conn.getAttribute(opt.get(), option);
        }catch(MBeanException e){
            log.error(e.getStackTrace().toString());
            e.getStackTrace();
        }catch(AttributeNotFoundException e){
            log.error(e.getStackTrace().toString());
            e.getStackTrace();
        }catch(InstanceNotFoundException e){
            log.error(e.getStackTrace().toString());
            e.getStackTrace();
        }catch(ReflectionException e){
            log.error(e.getStackTrace().toString());
            e.getStackTrace();
        }catch(IOException e){
            log.error(e.getStackTrace().toString());
            e.getStackTrace();
        }finally{
            return Optional.ofNullable(value);
        }        
        
    }
    
    private Optional<Object> getAttribute(ObjectName objName , String option){
        Object value = null;      
        if(conn == null){
            log.error("jmx connection is null");
            return Optional.empty();
        }
        
        try{
            value = conn.getAttribute(objName, option);
        }catch(MBeanException e){
            log.error(e.getStackTrace().toString());
            e.getStackTrace();
        }catch(AttributeNotFoundException e){
            log.error(e.getStackTrace().toString());
            e.getStackTrace();
        }catch(InstanceNotFoundException e){
            log.error(e.getStackTrace().toString());
            e.getStackTrace();
        }catch(ReflectionException e){
            log.error(e.getStackTrace().toString());
            e.getStackTrace();
        }catch(IOException e){
            log.error(e.getStackTrace().toString());
            e.getStackTrace();
        }finally{
            return Optional.ofNullable(value);
        }     
    }*/
    
}
