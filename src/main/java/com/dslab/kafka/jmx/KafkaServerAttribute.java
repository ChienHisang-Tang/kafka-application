/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.jmx;


import com.dslab.kafka.app.TopicList.Topic;
import java.io.IOException;
import java.util.Optional;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.util.List;
/**
 *
 * @author 翔翔
 */
public class KafkaServerAttribute {
    
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
      
    public static class MsglnCountPerSec  implements JmxAttribute{
        
        private MBeanServerConnection conn;
        private ObjectName indicator;
        private String attr;
        private String topic;
        
        public MsglnCountPerSec(MBeanServerConnection connection , Topic topic){
            this.conn = connection;
            this.topic = topic.toString();
            this.attr = "Count";
            this.indicator = new IndicatorPool(this.topic).getMessagesInPerSecIndicator();
        }
                
        @Override
        public Optional<Integer> getAttribute() {
            Object o = null;
            int v = (int)getConnAttribute(o , this.conn , this.indicator , this.attr).get();
            return Optional.of(v);
        }
                
    }
    
    public static class MsgInTpsPerSec implements JmxAttribute{
        
        private MBeanServerConnection conn;
        private ObjectName indicator;
        private String attr;
        private String topic;     
        
        public MsgInTpsPerSec(MBeanServerConnection connection , Topic topic){
            this.conn = connection;
            this.topic = topic.toString();
            this.attr = "OneMinuteRate";
            this.indicator = new IndicatorPool(this.topic).getMsgInTpsPerSecIndicator();
        }
        
        @Override
        public Optional<Double> getAttribute() {
            Object o = null;
            double v = (double)getConnAttribute(o , this.conn , this.indicator , this.attr).get();
            return Optional.of(v);            
        }
               
    }
    
    
    
    
    
    

    

}
