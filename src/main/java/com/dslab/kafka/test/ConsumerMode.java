/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.test;

//dslab
import com.dslab.kafka.para.HostIp;
import com.dslab.kafka.para.KafkaSerialClassName;
//java lib
import java.util.Properties;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
//kafka lib
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
/**
 *
 * @author 唐健翔
 */
public class ConsumerMode {
    Properties prop;
    public ConsumerMode(){
        prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HostIp.KAFKA_SERVER01_IP_PORT);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaSerialClassName.stringDeserializer);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSerialClassName.stringDeserializer);       
    }
    
    public void singleConsumerSingleGroup(String topicName , boolean haveWrite) throws IOException{
        this.prop.put(ConsumerConfig.GROUP_ID_CONFIG , "group01");
        KafkaConsumer<String , String> consumer = new KafkaConsumer<String , String>(prop);
        consumer.subscribe(Arrays.asList(topicName));
        
        if(haveWrite){
            String myFilePath = "/opt/kafkaCode/log/consumer.log";           
            File myFile = new File(myFilePath);
            if(!myFile.exists()){
                myFile.getParentFile().mkdirs();
                try{
                    myFile.createNewFile();
                }catch(IOException e){
                    e.printStackTrace();
                }
            }           
            FileOutputStream fout = new FileOutputStream(myFile);
            BufferedWriter writeDown = new BufferedWriter(new OutputStreamWriter(fout));            
            while(true){
                ConsumerRecords<String , String> records = consumer.poll(100);
                for(ConsumerRecord<String , String>record : records){
                    writeDown.write(record.offset() + " / " + record.key() + " / " + record.value());
                    writeDown.newLine();
                    System.out.println(record.offset() + " / " + record.key() + " / " + record.value());
                }
                writeDown.flush();
            }
        }else{
            while(true){
                ConsumerRecords<String , String> records = consumer.poll(100);
                for(ConsumerRecord<String , String> record : records){
                    System.out.println("Offset:" + record.offset() + " / " + "Topic:" + record.topic() + " / " + "Partition:" + record.partition()+" / "+"FileName:"+record.key());
                }
            }
        }     
    }
    
}
