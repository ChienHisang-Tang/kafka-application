/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.app;


//kafka lib
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
//java lib
import java.util.Properties;
import java.util.Arrays;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedWriter;
/**
 *
 * @author 工研翔翔哥
 */
public class ConsumerApplication {
    private String selection;
    private int consumerUse;
    private int consumerGroupUse;
    
    protected enum ConsumerMode{
        SINGLE_CONSUMER , MULTI_CONSUMER_SINGLE_GROUP , MULTI_CONSUMER_MULTI_GROUP;
    }
     
    public ConsumerApplication(ConsumerMode selection){
        this.selection = selection.toString();
        if(selection.equals("SINGLE_CONSUMER")){
            this.consumerUse = 1;
            this.consumerGroupUse = 1;
        }
         if(selection.equals("MULTI_CONSUMER_SINGLE_GROUP")){
            this.consumerUse = 2;
            this.consumerGroupUse = 1;
        }
        if(selection.equals("MULTI_CONSUMER_MULTI_GROUP")){
            this.consumerUse = 2;
            this.consumerGroupUse = 2;
        }     
    }
    
    public ConsumerApplication(ConsumerMode selection , int consumerAmount) throws Exception{
        
        if(consumerAmount <= 0){
            throw new Exception();
        }
        
        this.selection = selection.toString();
         if(selection.equals("SINGLE_CONSUMER")){
            this.consumerUse = 1;
            this.consumerGroupUse = 1;
        }
        if(selection.equals("MULTI_CONSUMER_SINGLE_GROUP")){
            this.consumerUse = consumerAmount;
             this.consumerGroupUse = 1;
        }
        if(selection.equals("MULTI_CONSUMER_MULTI_GROUP")){
            this.consumerUse = consumerAmount;
            this.consumerGroupUse = 2;
        }
            
    }
    
    public ConsumerApplication(ConsumerMode selection , int consumerAmount , int groupAmount) throws Exception{
        
        if(consumerAmount <= 0 || groupAmount <= 0){
            throw new Exception();
        }
        
        this.selection = selection.toString();
         if(selection.equals("SINGLE_CONSUMER")){
            this.consumerUse = 1;
            this.consumerGroupUse = 1;
        }
        if(selection.equals("MULTI_CONSUMER_SINGLE_GROUP")){
            this.consumerUse = consumerAmount;
            this.consumerGroupUse = 1;
        }
        if(selection.equals("MULTI_CONSUMER_MULTI_GROUP")){
            this.consumerUse = consumerAmount;
            this.consumerGroupUse = groupAmount;
        }
    }
    
    public void run(){
        switch(this.selection){
            case "SINGLE_CONSUMER":
                try{
                    singleConsumer(true);
                }catch(IOException ioexp){
                    System.out.println(ioexp);
                }finally{
                    break;
                }
            case "MULTI_CONSUMER_SINGLE_GROUP":
                break;
            case "MULTI_CONSUMER_MULTI_GROUP":
                break;
        }
    }
    
    private void singleConsumer(boolean writeOrNot) throws IOException{
        String groupID = "singleGroup";
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ServerIP.KAFKA_SERVER01);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG , groupID);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        
        KafkaConsumer<String , String> consumer = new KafkaConsumer<String , String>(prop);
        consumer.subscribe(Arrays.asList(TopicList.Topic.TOPIC_ONE.toString()));
        
        if(writeOrNot){
            String myDir = "/opt/consumer_log";
            File myFile = new File(myDir);
            if(!myFile.isDirectory())
                myFile.mkdir();
            
            BufferedWriter writeDown = new BufferedWriter(new FileWriter(myDir + "/consumer.log"));            
            while(true){
                ConsumerRecords<String , String> records = consumer.poll(100);
                for(ConsumerRecord<String , String>record : records){
                    writeDown.write(record.offset() + " / " + record.key() + " / " + record.value());
                    System.out.println(record.offset() + " / " + record.key() + " / " + record.value());
                }
            }
        }else{
            while(true){
                ConsumerRecords<String , String> records = consumer.poll(100);
                for(ConsumerRecord<String , String> record : records){
                    System.out.println(record.offset() + " / " + record.key() + " / " + record.value());
                }
            }
        }

    }
    
    private void multiThreadConsumer(int consumerAmount , int groupAmount){
        
    }
}
