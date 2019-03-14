/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.app;

import com.dslab.kafka.jmx.JmxManager;
import com.dslab.kafka.connect.Hdfs;
import com.dslab.kafka.other.FileCreator;
import com.dslab.kafka.other.FileRemoteTransfer;
import com.dslab.kafka.para.HostIp;
import com.dslab.kafka.test.ProducerMode;
import com.dslab.kafka.test.ConsumerMode;
//java lib
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Optional;
import javax.management.MBeanServerConnection;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
/**
 *
 * @author 工研翔翔哥
 */
public class Entry {
    
    private static final String configFile = "./akConfig.properties";
     
    public static void main(String[] args) throws IOException{
        
        Properties prop = new Properties();
        try{
            prop.load(new FileInputStream(configFile));
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
        
        String instr = prop.getProperty("instr", "not support");
        
        if(instr.equals("producer_test")){
            String numMessage = prop.getProperty("numMsg");
            String topicName = prop.getProperty("producerTopicName");
            ProducerMode producer = new ProducerMode();
            producer.singleSyn(topicName, Integer.parseInt(numMessage));
        }else if(instr.equals("producer_hdfs")){
            String hdfsDirPath = prop.getProperty("hdfsDirPath");
            int polling = Integer.valueOf(prop.getProperty("pollingInterval"));
            int batchSize = Integer.valueOf(prop.getProperty("mainProducerBatchSize"));
            int bufferSize = Integer.valueOf(prop.getProperty("mainProducerBufferSize"));
            ProducerApplication producer = new ProducerApplication();
            try{
                //producer.sendHdfsFilesPermanentBySingleProducerInSyn(TopicList.Topic.TOPIC_TWO.toString(), hdfsDirPath, 100 , 1000 , 33554432 , 33554432 , 100663296);
                producer.sendHdfsFilesPermanentBySingleProducerInAsyn("byAPI", hdfsDirPath, polling, batchSize , batchSize, bufferSize);
            }catch(InterruptedException e){
                e.printStackTrace();
            }          
        }else if(instr.equals("producer_hds_scp")){
            String localPath = prop.getProperty("localPath");
            String folderName = "xfolder";
            String remoteHost = "192.168.103.173";
            String user = "root";
            String remotePath = "/opt/";
            
            FileRemoteTransfer rmt = new FileRemoteTransfer();
            //get file from hds
            rmt.getFileFromHds();
            //get file from scp
            rmt.scpGetFolder(remoteHost, user, remotePath + folderName + "/" , localPath);
            //use producer to send
            ProducerApplication producerApp = new ProducerApplication();
            producerApp.sendLocalFilesBySingleProducer("byAPI",false,localPath,true);
            
        }else if(instr.equals("consumer_test")){
            String topic = prop.getProperty("subscribeTopicName");
            try{
                ConsumerMode consumer = new ConsumerMode();
                consumer.singleConsumerSingleGroup(topic, false);
            }catch(IOException e){
                e.printStackTrace();
            }         
        }else if(instr.equals("consumer_hdfs")){
            ConsumerApplication consumerApp = new ConsumerApplication();
            consumerApp.getFilesPermanentBySingleConsumer("byAPI");
        }else if(instr.equals("jmx")){
            List<String> brokerList = new ArrayList<>();
            brokerList.add(HostIp.KAFKA_SERVER01_IP);
            brokerList.add(HostIp.KAFKA_SERVER02_IP);
            brokerList.add(HostIp.KAFKA_SERVER03_IP);
            JmxManager jm = new JmxManager(brokerList , HostIp.KAFKA_SERVER01_JMX_PORT);
            if(jm.connectJmx()){
                System.out.println(jm.getMsgInCountPerSec("byAPI")); 
            }
        }else if(instr.equals("topicCreate")){          
            String topicName = prop.getProperty("createTopicName");
            int numPart = Integer.valueOf(prop.getProperty("numPartition")) ;
            short numRep = Short.valueOf(prop.getProperty("numReplica"));
            BrokerOperation bp = new BrokerOperation(HostIp.KAFKA_SERVER02_IP_PORT);
             try{           
                 bp.createTopics(topicName, numPart, numRep);
             }catch(Exception e){
                 System.out.println(e);
            }finally{
                bp.close();
            }     
        }else if(instr.equals("topicList")){
            BrokerOperation bp = new BrokerOperation(HostIp.KAFKA_SERVER02_IP_PORT);
            try{
                String[] list = bp.listAllTopics();
                System.out.println(list.length);
                for(String str : list){
                    System.out.println(str);
                }
            }catch(Exception e){
                e.printStackTrace();
            }finally{
                bp.close();
            }
        }else if(instr.equals("topicDelete")){
            BrokerOperation bp = new BrokerOperation(HostIp.KAFKA_SERVER02_IP_PORT);
            try{
                String[] list = bp.listAllTopics();
                for(String str : list){
                    if(str.equals("byAPI")){
                        continue;
                    }
                    bp.deleteTopic(str);
                }
            }catch(Exception e){
                e.printStackTrace();
            }finally{
                bp.close();
            }            
        }else if(instr.equals("createFile")){
            String path = prop.getProperty("FilePath");
            String fileSize = prop.getProperty("FileSize");
            String fileUnit = prop.getProperty("FileUnit");
            FileCreator creator = new FileCreator();
            try{
                creator.createFile(path, Integer.parseUnsignedInt(fileSize), fileUnit.charAt(0));
            }catch(IOException e){
                e.printStackTrace();
            }
        }else if(instr.equals("createFiles")){
            String folderPath = prop.getProperty("FolderPath");
            String folderSize = prop.getProperty("FolderSize");
            String folderUnit = prop.getProperty("FolderUnit");
            String fileSize = prop.getProperty("FolderEachFileSize");
            String fileUnit = prop.getProperty("FolderEachFileUnit");
           FileCreator creator = new FileCreator();
           creator.setFileContentPerLine("This a line content This a line content This a line content This a line content");          
           creator.createFiles(folderPath, Integer.parseUnsignedInt(folderSize), folderUnit.charAt(0), Integer.parseUnsignedInt(fileSize), fileUnit.charAt(0));
        }else if(instr.equals("putFilesToHdfs")){
            String fileValue = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
            Hdfs connect = new Hdfs("");
            for(int i = 11 ; i < 100 ; i++){
                try{
                    connect.putFile("/myFolder/putfile" + i + ".txt", fileValue.getBytes());
                }catch(IOException e){
                    e.printStackTrace();
                }
            }

        }else if(instr.equals("scpSendFile")){
            FileRemoteTransfer rmt = new FileRemoteTransfer();
            rmt.scpSendFile("192.168.103.38", "root", "/opt/kafkaCode/" , "/opt/kafkaCode/log/" , "file5.txt");
        }else if(instr.equals("scpGetFile")){
            FileRemoteTransfer rmt = new FileRemoteTransfer();
            rmt.scpReciveFile("192.168.103.38", "root", "/opt/kafkaCode/" , "/opt/kafkaCode/log/" , "file6.txt");
        }else{
            System.out.println(instr);         
        }
      
    }
}
