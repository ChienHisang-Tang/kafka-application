/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.jmx;

//java lib
import java.util.Optional;
import java.util.Set;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.util.Map;
import java.util.HashMap;
//Exception
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import java.io.IOException;


/**
 *
 * @author 翔翔
 */
public abstract class KafkaServerAttribute {
    
    protected MBeanServerConnection connection = null;
    protected ObjectName indicator = null;
    protected String attribute = null;
    protected String topic = null;
    
    public abstract String getAttribute();
    
    private static Optional<Object> getConnAttribute(Object o , MBeanServerConnection conn , ObjectName indicator , String attr){
        try{
            o = conn.getAttribute(indicator, attr);
        }catch(MBeanException e){
            e.printStackTrace();
        }catch(AttributeNotFoundException e){
            e.printStackTrace();
        }catch(InstanceNotFoundException e){
            e.printStackTrace();
        }catch(ReflectionException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
        
        return Optional.ofNullable(o);
    }
      
    protected static class MsglnCountPerSec extends KafkaServerAttribute{
               
        public MsglnCountPerSec(MBeanServerConnection connection , String topic){
            this.connection = connection;
            this.topic = topic.toString();
            this.attribute = "Count";
            this.indicator = new IndicatorPool(this.topic).getMessagesInPerSecIndicator();
        }
                
        @Override
        public String getAttribute() {
            Object o = null;
            long v = (long)getConnAttribute(o , this.connection , this.indicator , this.attribute).get();
            return String.valueOf(v);
        }
                
    }
    
    protected static class MsgInTpsPerSec extends KafkaServerAttribute{
           
        public MsgInTpsPerSec(MBeanServerConnection connection , String topic){
            this.connection = connection;
            this.topic = topic.toString();
            this.attribute = "OneMinuteRate";
            this.indicator = new IndicatorPool(this.topic).getMsgInTpsPerSecIndicator();
        }
        
        @Override
        public String getAttribute() {
            Object o = null;
            double v = (double)getConnAttribute(o , this.connection , this.indicator , this.attribute).get();
            return String.valueOf(v);
        }
               
    }
    
    protected static class TopicEndOffset extends KafkaServerAttribute{
        
        private String keyProperty = "partition";
        
        public TopicEndOffset(MBeanServerConnection connection , String topic){
            this.connection = connection;
            this.topic = topic.toString();
            this.attribute = "Value";
            this.indicator = new IndicatorPool(this.topic).getEndOffsetObjectsIndicator();       
        }
        
        private int getPartitionId(ObjectName indi){
            String id = indi.getKeyProperty(keyProperty);
            return Integer.valueOf(id);
        }
        
        private Optional<Set<ObjectName>> getIndicators(){
            Set<ObjectName> indicators = null;
            try{
                indicators = this.connection.queryNames(this.indicator, null);
            }catch(IOException e){
                e.printStackTrace();
            }
            return Optional.ofNullable(indicators);
        }
        
        @Override
        public String getAttribute() {
            Set<ObjectName> indicators = this.getIndicators().get();
            Map<Integer,Long> map = new HashMap<>();
            for(ObjectName in : indicators){
                Object o = null;
                int pid = getPartitionId(in);
                Optional<Object> obj = getConnAttribute(o , this.connection , in , this.attribute);
                if(obj.isPresent())
                    map.put(pid, (long)obj.get());
            }
            return map.toString();
        }
    
    }
    
    
}
