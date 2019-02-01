/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.app;
//import com.dslab.kafkaApp.ApplicationOptions.ConsumerMode;
import com.dslab.kafka.app.ProducerApplication.ProducerMode;
import com.dslab.kafka.jmx.JmxAttribute;
import com.dslab.kafka.jmx.JmxConnection;
import com.dslab.kafka.jmx.KafkaServerAttribute;
import com.dslab.kafka.app.TopicList.Topic;
//java lib
import java.util.Optional;
import javax.management.MBeanServerConnection;
/**
 *
 * @author 工研翔翔哥
 */
public class Entry {
    public static void main(String[] args){
         /*ProducerApplication pa = new ProducerApplication(ProducerMode.SINGLE_ASYN);
         pa.run();*/
         /*ProducerApplication pa2 = new ProducerApplication(ProducerMode.SINGLE_SYN);
         pa2.run();   */
         /*ConsumerApplication ca = new ConsumerApplication(ConsumerMode.SINGLE_CONSUMER);
         ca.run();*/
         /*BrokerOperation bp = new BrokerOperation(ServerIP.KAFKA_SERVER01);
         try{           
             bp.createTopics(bp.start(),"byAPI", 3, (short)3);
         }catch(Exception e){
              System.out.println(e);
         }finally{
             bp.close();
         }*/
         /*ProducerApplication pa3 = new ProducerApplication(ProducerMode.MULTI_SYN , 2);
         pa3.run();*/
         
         Optional<MBeanServerConnection> conn =  JmxConnection.create("192.168.103.50", "9999");
         JmxAttribute attr = new KafkaServerAttribute.MsglnCountPerSec(conn.get() , Topic.TOPIC_ONE);
         int result = (int)attr.getAttribute().get();
         System.out.println(result);

    }
}
