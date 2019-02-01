/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.app;


//kafka client lib
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
//java lib
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.List;
import java.util.ArrayList;


/**
 *
 * @author 工研翔翔
 */
public class ProducerApplication {
    
    private String selection;
    private int multiProducerUse = 2;
    //compressions
    private String compressionMode = "none";
    //topic
    private String myTopic = TopicList.Topic.TOPIC_ONE.toString();
    
    protected enum ProducerMode{
        SINGLE_ASYN , SINGLE_SYN , MULTI_ASYN , MULTI_SYN;
    }
    
    protected enum ProducerCompression{
        GZIP , SNAPPY , LZ4;
    }
     
    
    public ProducerApplication(ProducerMode selection){
        this.selection = selection.toString();
    }
    
    public ProducerApplication(ProducerMode selection , int producerAmount){
        this.selection = selection.toString();       
        if(this.selection.equals("MULTI_ASYN") || this.selection.equals("MULTI_SYN"))
             this.multiProducerUse = producerAmount;      
    }
    
    public ProducerApplication(ProducerMode selection , ProducerCompression com){
        this.selection = selection.toString();
        this.compressionMode = com.toString();
    }
    
    public ProducerApplication(ProducerMode selection , ProducerCompression com , int producerAmount){
        this.selection = selection.toString();
        this.compressionMode = com.toString();
        if(this.selection.equals("MULTI_ASYN") || this.selection.equals("MULTI_SYN"))
            this.multiProducerUse = producerAmount;  
    }
    
    public int getCurrentMultiProducerAmount(){
        return multiProducerUse;
    }
    
    public void run(){
        switch(this.selection){
            case "SINGLE_ASYN": runSingleAsyn(); break;
            case "SINGLE_SYN": runSingleSyn(); break;
            case "MULTI_ASYN": runMultiAsyn(this.multiProducerUse); break;
            case "MULTI_SYN": runMultiSyn(multiProducerUse); break;
        }
    }
    
    private void runSingleAsyn(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , ServerIP.KAFKA_SERVER01);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , ApplicationUsefulPathString.stringSerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , ApplicationUsefulPathString.stringSerializer);
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        
        
        
        Producer<String,String> producer = new KafkaProducer<>(props);
        System.out.println("runSingleAsyn------------------------------------------->start");
        for(int i = 0 ; i < 500 ; i++){
            ProducerRecord myRecord = new ProducerRecord(TopicList.Topic.TOPIC_ONE.toString() , Integer.toString(i) , Integer.toBinaryString(i));
            Callback ck = new Callback(){
                @Override
                public void onCompletion(RecordMetadata rm, Exception exception){
                    if(exception == null){
                        System.out.println(rm.timestamp() + "/"+ rm.topic() +"/"+ rm.partition() +"/"+ rm.offset());
                    }else{
                        System.out.println(rm.timestamp()  + "/" + exception);
                    }
                }
            };
            producer.send(myRecord, ck);
            System.out.println(i);
        }
        producer.flush();
        producer.close();
    }
    
    private void runSingleSyn(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , ServerIP.KAFKA_SERVER01);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , ApplicationUsefulPathString.stringSerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , ApplicationUsefulPathString.stringSerializer);
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        
        
        Producer<String,String> producer = new KafkaProducer<>(props);
        try{
            System.out.println("runSingleSyn------------------------------------------->start");
            for(int i = 0 ; i < 500 ; i++){
                ProducerRecord myRecord = new ProducerRecord(TopicList.Topic.TOPIC_ONE.toString() , Integer.toString(i) , Integer.toBinaryString(i));
                Future<RecordMetadata> future = producer.send(myRecord);
                RecordMetadata metadata = future.get();
                System.out.println(Integer.toString(i) + metadata.timestamp() + "/"+ metadata.topic() +"/"+ metadata.partition() +"/"+ metadata.offset());
            }
        }catch(Exception exception){
            System.out.println(exception);
        }finally{
            producer.flush();
            producer.close(); 
        }
        
    }
    
    private void runMultiAsyn(int producerAmount){
                     
        class AsynThread implements Runnable{
            private  String threadName;
            private  String topicName;
            private  Properties myProps;
            private  Producer myProducer;
            
            public AsynThread(String threadName , String topic){
                this.threadName = threadName;
                this.topicName = topic;
                myProps = new Properties();
                myProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , ServerIP.KAFKA_SERVER01);
                myProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , ApplicationUsefulPathString.stringSerializer);
                myProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , ApplicationUsefulPathString.stringSerializer);
                myProps.put(ProducerConfig.ACKS_CONFIG, "-1");
                myProps.put(ProducerConfig.RETRIES_CONFIG, 3);
                myProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
                myProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
                myProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
                myProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
                myProducer = new KafkaProducer<String , String>(myProps);
            }
             
            @Override
            public void run(){
                for(int i = 0 ; i < 2000 ; i++){
                    ProducerRecord myRecord = new ProducerRecord(this.topicName , this.threadName , Integer.toBinaryString(i));
                    this.myProducer.send(myRecord, new Callback(){
                        @Override
                        public void onCompletion(RecordMetadata m, Exception e) {
                            if(e == null){
                                //System.out.println(m.topic());
                            }else{
                                System.err.println(e);
                            }
                        }                     
                    });//callback end
                    System.out.println(this.threadName + ": " + i);
                }
                this.myProducer.close();
            }
            
        }

        List<String> topicList = TopicList.getAllTopic();
        
        for(int i = 0 ; i < producerAmount ; i++){
            new Thread(new AsynThread("Thread(" + i + ")" , topicList.get( i % topicList.size())) ).start();
        }
        
    }
    
    private void runMultiSyn(int producerAmount){
        
         class SynThread implements Runnable{
             
             private String threadName;
             private String topicName;
             private Properties myProps;
             private Producer<String , String> myProducer;
             
             public SynThread(String name , String topic){
                 this.threadName = name;
                 this.topicName = topic;
                 myProps = new Properties();
                 myProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , ServerIP.KAFKA_SERVER01);
                 myProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , ApplicationUsefulPathString.stringSerializer);
                 myProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , ApplicationUsefulPathString.stringSerializer);
                 myProps.put(ProducerConfig.ACKS_CONFIG, "-1");
                 myProps.put(ProducerConfig.RETRIES_CONFIG, 3);
                 myProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
                 myProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
                 myProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
                 myProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
                 myProducer = new KafkaProducer<>(myProps);
             }
                        
             @Override
             public void run(){
                 for(int i = 0 ; i < 2000 ; i++){
                     ProducerRecord record = new ProducerRecord(this.topicName , this.threadName , Integer.toBinaryString(i));
                     try{
                          myProducer.send(record).get();
                          System.out.println(this.threadName + ": " + i);
                     }catch(Exception e){
                         System.err.println(e);
                     }
                 }
                 this.myProducer.close();
             }
         }
         
         List<String> topicList = TopicList.getAllTopic();
         
         for(int i = 0 ; i < producerAmount ; i++)
             new Thread(new SynThread( "Thread(" + i + ")" ,  topicList.get( i % topicList.size()))).start();         
    }
       
}
