/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.test;


//dslab
import com.dslab.kafka.para.KafkaSerialClassName;
import com.dslab.kafka.para.HostIp;
//java
import java.util.Properties;
import java.util.concurrent.Future;
//kafka
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
/**
 *
 * @author 唐健翔
 */
public class ProducerMode {
    
    protected Properties producerProp;
    
    protected enum compression{
        GZIP,SNAPPY,LZ4;
    }
    
    public ProducerMode(){
        producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , HostIp.KAFKA_SERVER01_IP_PORT);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , KafkaSerialClassName.stringSerializer);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , KafkaSerialClassName.stringSerializer);
        producerProp.put(ProducerConfig.ACKS_CONFIG, "-1");
        producerProp.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProp.put(ProducerConfig.BATCH_SIZE_CONFIG, 3238400);
        producerProp.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        producerProp.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        producerProp.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
    }
    
    public void singleAsyn(String topicName , int numMessage){
        Producer<String,String> producer = new KafkaProducer<>(this.producerProp);
        System.out.println("runSingleAsyn------------------------------------------->start");
        for(int i = 0 ; i < numMessage ; i++){
            String key = Integer.toString(i);
            String value = Integer.toBinaryString(i);
            ProducerRecord myRecord = new ProducerRecord(topicName , key , value);
            producer.send(myRecord, new Callback(){
                @Override
                public void onCompletion(RecordMetadata rm, Exception e){
                    if(e == null){
                        System.out.println("Time:"+rm.timestamp()+"|"+"Topic:"+rm.topic()+"|"+"Partition:"+rm.partition()+"|"+"Offset:"+rm.offset());
                    }else{
                        System.out.println(rm.timestamp()+":");
                        e.printStackTrace();
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
    
    public void singleAsynWithCompression(String topicName){
        
    }
    
    public void singleSyn(String topicName , int numMessage){
        Producer<String,String> producer = new KafkaProducer<>(this.producerProp);
        try{
            System.out.println("runSingleSyn------------------------------------------->start");
            for(int i = 0 ; i < numMessage ; i++){
                String key = Integer.toString(i);
                String value = Integer.toHexString(i);
                ProducerRecord myRecord = new ProducerRecord(topicName , key , value);
                Future<RecordMetadata> future = producer.send(myRecord);
                RecordMetadata metadata = future.get();
                System.out.println("Time:"+metadata.timestamp()+"|"+"Topic:"+metadata.topic()+"|"+"Partition:"+metadata.partition()+"|"+"Offset:"+metadata.offset());              
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            producer.flush();
            producer.close();
        }
    }
    
    public void singleSynWithCompression(String topicName){
    
    }
    
    public void multiAsyn(String topicName, int numProducer , int numMessage){
        class AsynThread implements Runnable {
            private String topicName;
            private String threadName;
            private int numMessage;
            private Producer myProducer;
            public AsynThread(String topicName , String threadName , int numMessage){
                this.topicName = topicName;
                this.threadName = threadName;
                this.numMessage = numMessage;
                this.myProducer = new KafkaProducer<String,String>(producerProp);
            }
            @Override
            public void run(){
                for(int i = 0 ; i < this.numMessage ; i++){
                    String key = this.threadName;
                    String value = Integer.toHexString(i);
                    ProducerRecord myRecord = new ProducerRecord(this.topicName , key , value);
                    this.myProducer.send(myRecord, new Callback(){
                        @Override
                        public void onCompletion(RecordMetadata rm, Exception e) {
                            if(e == null){
                                System.out.println("Time:"+rm.timestamp()+"|"+"Topic:"+rm.topic()+"|"+"Partition:"+rm.partition()+"|"+"Offset:"+rm.offset());
                            }else{
                                System.out.println("TimeStamp:" + rm.timestamp()+":");
                                e.printStackTrace();
                            }
                        }
                    });
                }
                this.myProducer.flush();
                this.myProducer.close();
            }
        }
        for(int i = 0 ; i < numProducer ; i++){
            new Thread(new AsynThread(topicName,"Thread("+i+")" , (int)(numMessage/numProducer))).start();
        }
    }
    
    public void multiAsynWithCompression(String topicName){
    
    }
    
    public void multiSyn(String topicName , int numProducer , int numMessage){
        class SynThread implements Runnable{
            private String topicName;
            private String threadName;
            private int numMessage;
            private Producer myProducer;
            public SynThread(String topicName , String threadName , int numMessage){
                this.topicName = topicName;
                this.threadName = threadName;
                this.numMessage = numMessage;
                this.myProducer = new KafkaProducer<String,String>(producerProp);
            }
            @Override
            public void run() {
                try{
                    for(int i = 0 ; i < this.numMessage ; i++){
                        String key = this.threadName;
                        String value = Integer.toHexString(i);
                        ProducerRecord myRecord = new ProducerRecord(this.topicName , key , value);
                        Future<RecordMetadata>  future = myProducer.send(myRecord);                       
                        RecordMetadata metadata = future.get();
                        System.out.println("Time:"+metadata.timestamp()+"|"+"Topic:"+metadata.topic()+"|"+"Partition:"+metadata.partition()+"|"+"Offset:"+metadata.offset());
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }finally{
                    this.myProducer.flush();
                    this.myProducer.close();
                }
            }
        
        }
        for(int i = 0 ; i < numProducer ; i++){
            new Thread(new SynThread(topicName , "Thread("+i+")" , (int)(numMessage/numProducer))).start();
        }
    }
    
}
