/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.app;

//dslab
import com.dslab.kafka.para.KafkaSerialClassName;
import com.dslab.kafka.para.HostIp;
import com.dslab.kafka.connect.Hdfs;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Date;
//hadoop
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
/**
 *
 * @author 工研翔翔
 */
public class ProducerApplication {
    
    //ver2
    private boolean queueSelector;   //true:queue1   false:queue2
    private  int totalNumFiles = 0;
    private boolean isHdfsFilesReadyInNextQueue = false;
    
    protected enum ProducerCompression{
        GZIP , SNAPPY , LZ4;
    }
           
    public void sendLocalFilesBySingleProducer(String topicName , boolean readMode , String loadPath , boolean isSyn){
        String path = loadPath;
        boolean readDetailOrNot = readMode;    //detail : true   not detail : false 
        File producerFile;
        List<String> filesList = new ArrayList<>();
            
        producerFile = new File(path);
        
        if(!producerFile.exists()){
            System.out.println(path + " not found");
            return;
        }
        
        if(producerFile.isDirectory()){
            //read dir 
            filesList = getDirectorFiles(producerFile);          
        }else if(producerFile.isFile()){
            //read file
            filesList.add(producerFile.getName());
        }else{
            System.out.println(path + " is not dir or file");
            return;
        }
        
        if(!filesList.isEmpty()){
            String encoding = "UTF-8";
            int numFile = filesList.size();
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , HostIp.KAFKA_SERVER01_IP_PORT);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , KafkaSerialClassName.stringSerializer);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , KafkaSerialClassName.stringSerializer);
            producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
            producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
            producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
            producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
            producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
            producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
            Producer<String,String> producer = new KafkaProducer<>(producerProps);         
            
            if(readDetailOrNot){
                //read line by line
                System.out.println("in line to line");
            }else{
                //read file in a stream
                if(isSyn){
                    //syn
                    for(int i = 0 ; i < numFile ; i++){
                        System.out.println("------------------------"+filesList.get(i) + "--------------------->>>");
                        File myFile = new File(filesList.get(i));
                        Long fileLen = myFile.length();
                        byte[] fileByteBuffer = new byte[fileLen.intValue()]; 
                        int bytesRead = -1;
                        try{
                            //load file to buffer
                            FileInputStream fsin = new FileInputStream(myFile);
                            do{
                                bytesRead = fsin.read(fileByteBuffer);
                            }while(bytesRead != -1);                                           
                            fsin.close();
                            //send file                    
                            ProducerRecord myRecord = new ProducerRecord(topicName , Integer.toString(i) , new String(fileByteBuffer , encoding));
                            Future<RecordMetadata> future = producer.send(myRecord);
                            RecordMetadata metadata = future.get();
                            System.out.println(Integer.toString(i) + "|"+"TimeStamp:"+ metadata.timestamp() + "|"+"Topic:"+ metadata.topic() +"|"+"Partition:"+ metadata.partition() +"|"+"Offset:"+ metadata.offset());                   
                        }catch(FileNotFoundException e){
                            e.printStackTrace();
                        }catch(IOException e){
                            e.printStackTrace();
                        }catch(InterruptedException e){
                            e.printStackTrace();
                        }catch(ExecutionException e){
                            e.printStackTrace();
                        }                   
                    }                   
                }else{
                    //asyn
                    for(int i = 0 ; i < numFile ; i++){
                        System.out.println("------------------------"+filesList.get(i) + "--------------------->>>");
                        File myFile = new File(filesList.get(i));
                        Long fileLen = myFile.length();
                        byte[] fileByteBuffer = new byte[fileLen.intValue()]; 
                        int bytesRead = -1;
                        try{
                            //load file to buffer
                            FileInputStream fsin = new FileInputStream(myFile);
                            do{
                                bytesRead = fsin.read(fileByteBuffer);
                            }while(bytesRead != -1);                                           
                            fsin.close();
                            //sendFIle
                            ProducerRecord myRecord = new ProducerRecord(topicName , Integer.toString(i) , new String(fileByteBuffer , encoding));
                            producer.send(myRecord, new Callback() {
                                @Override
                                public void onCompletion(RecordMetadata metadata, Exception e) {
                                    if(e == null){
                                        System.out.println("TimeStamp:"+ metadata.timestamp() + "|"+"Topic:"+ metadata.topic() +"|"+"Partition:"+ metadata.partition() +"|"+"Offset:"+ metadata.offset());
                                    }else{
                                        e.printStackTrace();
                                    }
                                }
                            });
                        }catch(FileNotFoundException e){
                            e.printStackTrace();
                        }catch(IOException e){
                            e.printStackTrace();
                        }
                    }
                }
            }
        }else{
            System.out.println("fileList is empty");
            return;
        }
    }
       
    private List<String> getDirectorFiles(File fileEntry){
        List<String> fileNameList = new ArrayList<>();
        for(File file : fileEntry.listFiles()){
            if(file.isDirectory()){
                List<String> tempList = getDirectorFiles(file);
                fileNameList.addAll(tempList);
            }else{
                fileNameList.add(file.getPath());
            }
        }
        return fileNameList;
    }
    
    public void sendHdfsFilesBySingleProducer(String topicName , String hdfsDirPath , boolean isSyn) throws IOException , FileNotFoundException{
        
        FileSystem fs;
        byte[] outFileValue;
        Properties producerProps = new Properties();
        
        Hdfs myHdfs = new Hdfs(hdfsDirPath);
        Path path = myHdfs.getHdfsPath();
        fs = myHdfs.getHdfsFileSystem().get();
        if(fs.isDirectory(path)){
            FileStatus[] fileList = fs.listStatus(path);
            if(fileList.length == 0){
                System.out.println("Not any file in this dir");
                return;
            }
            //initial producer        
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , HostIp.KAFKA_SERVER01_IP_PORT);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , KafkaSerialClassName.stringSerializer);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , KafkaSerialClassName.byteArraySerializer);
            producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
            producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
            producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
            producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
            producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
            producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);           
            Producer<String,Byte[]> producer = new KafkaProducer<>(producerProps);
            
            if(isSyn){
                int fileIndex = 0;
                 for(FileStatus file : fileList){
                    FSDataInputStream inputStream = fs.open(file.getPath());
                    outFileValue = IOUtils.toByteArray(inputStream);
                    ProducerRecord myRecord = new ProducerRecord(topicName , file.getPath().getName() , outFileValue);
                    try{
                        fileIndex++;
                        Future<RecordMetadata> future =  producer.send(myRecord);
                        RecordMetadata metadata = future.get();
                        //System.out.println(fileIndex + "|"+"TimeStamp:"+ metadata.timestamp() + "|"+"Topic:"+ metadata.topic() +"|"+"Partition:"+ metadata.partition() +"|"+"Offset:"+ metadata.offset());
                        System.out.println(fileIndex + ": " + myRecord.key().toString());
                    }catch(InterruptedException e){
                        e.printStackTrace();
                    }catch(ExecutionException e){
                        e.printStackTrace();
                    }             
                    inputStream.close();
               }               
            }else{
                for(FileStatus file : fileList){
                    FSDataInputStream inputStream = fs.open(file.getPath());
                    outFileValue = IOUtils.toByteArray(inputStream);
                    System.out.println(outFileValue.length);
                    ProducerRecord myRecord = new ProducerRecord(topicName , file.getPath().getName() , outFileValue);
                    producer.send(myRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if(e == null){
                                System.out.println(myRecord.key());
                            }else{
                                e.printStackTrace();
                            }
                        }
                    });
                    inputStream.close();
                }            
            }
      
        }else{
            return;
        }

    }
      
    //queueRemainPercent 0~100
    public void sendHdfsFilesPermanentBySingleProducerInSyn(String topicName , String hdfsDirPath , int queueRemainPercent , int pollingFileSystemTimeInMs , 
           int producerBatchSize , int producerRequestBrokerSize , int producerBufferSize) throws InterruptedException{  
        Map<String , byte[]> filesValueQueue_One = new HashMap<>();
        Map<String , byte[]>filesValueQueue_Two = new HashMap<>();
        List<String> filesNameQueue_One = new ArrayList<>();
        List<String> filesNameQueue_Two = new ArrayList<>();
        List<String> inFlightQueue = new ArrayList<>();
        List<String> destroyQueue = new ArrayList<>();
        Thread traceHdfsFileSys;
        Thread destroyHdfsFile;
        int currentQueueLen = 0;
        Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , HostIp.KAFKA_SERVER01_IP_PORT);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , KafkaSerialClassName.stringSerializer);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , KafkaSerialClassName.byteArraySerializer);
        producerProp.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProp.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProp.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);//10MB 10242880
        producerProp.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        producerProp.put(ProducerConfig.BUFFER_MEMORY_CONFIG, producerBufferSize);//32MB 33554432
        producerProp.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        producerProp.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, producerRequestBrokerSize); //10242880
        Producer<String , byte[]> myKafkaProducer;
        Hdfs myHdfs = new Hdfs(hdfsDirPath);
        Path hPath = myHdfs.getHdfsPath();
        FileSystem hfs = myHdfs.getHdfsFileSystem().get();
        
        try{
            if(!hfs.isDirectory(hPath)){
                System.out.println("is not folder path");
                return;
            }
        }catch(IOException e){
            e.printStackTrace();
        }
        
        
        class HdfsFileCapture implements Runnable{
            int queueMaxByte = producerBufferSize;
            FileSystem hfs = myHdfs.getHdfsFileSystem().get();
            @Override
            public void run(){
                try{
                    //FileSystem hfs = myHdfs.getHdfsFileSystem().get();
                    long accumulationByte = 0;
                    if(queueSelector){//表示producer正在用nameQueue_one
                        do{
                            FileStatus[] fileList = hfs.listStatus(hPath);
                            for(FileStatus file : fileList){
                                String fileName = file.getPath().getName();
                                if(fileName.contains("._COPYING_"))
                                    continue;
                                if(!destroyQueue.contains(fileName) && !inFlightQueue.contains(fileName) && !filesNameQueue_One.contains(fileName)){
                                    accumulationByte += hfs.getContentSummary(file.getPath()).getLength();
                                    if(accumulationByte >= this.queueMaxByte)
                                        break;
                                    FSDataInputStream inputStream = hfs.open(file.getPath());
                                    byte[] outValue = IOUtils.toByteArray(inputStream);
                                    filesNameQueue_Two.add(fileName);
                                    filesValueQueue_Two.put(fileName, outValue);
                                    inputStream.close();
                                }
                            }   
                            if(filesNameQueue_Two.size() == 0){
                                System.out.println("queue 2 checking");
                                Thread.sleep(pollingFileSystemTimeInMs);
                            }else{
                                accumulationByte = 0;
                                //hfs.close();
                                isHdfsFilesReadyInNextQueue = true;
                                System.out.println("queue 2 done");
                                break;
                            }                            
                        }while(true);                      
                    }else{//表示rpoducer正在用nameQueue_two
                        do{
                            FileStatus[] fileList = hfs.listStatus(hPath);
                            for(FileStatus file : fileList){
                                String fileName = file.getPath().getName();
                                if(fileName.contains("._COPYING_"))
                                    continue;
                                if(!destroyQueue.contains(fileName) && !inFlightQueue.contains(fileName) && !filesNameQueue_Two.contains(fileName)){
                                    accumulationByte += hfs.getContentSummary(file.getPath()).getLength();
                                    if(accumulationByte >= this.queueMaxByte)
                                        break;
                                    FSDataInputStream inputStream = hfs.open(file.getPath());
                                    byte[] outValue = IOUtils.toByteArray(inputStream);
                                    filesNameQueue_One.add(fileName);
                                    filesValueQueue_One.put(fileName, outValue);
                                    inputStream.close();
                                }
                            }
                            if(filesNameQueue_One.size() == 0){
                                System.out.println("queue 1 checking");
                                Thread.sleep(pollingFileSystemTimeInMs);
                            }else{
                                accumulationByte = 0;
                                //hfs.close();
                                isHdfsFilesReadyInNextQueue = true;
                                System.out.println("queue 1 done");
                                break;
                            }
                        }while(true);                     
                    }                
                }catch(IOException e){
                    e.printStackTrace();
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }
        }
        
        class sentHdfsFileDeleter implements Runnable{
            @Override
            public void run(){
                
            }
        }
        //firstTime, load data to queue one
        queueSelector = false;
        traceHdfsFileSys = new Thread(new HdfsFileCapture());
        traceHdfsFileSys.start();
        traceHdfsFileSys.join();
        queueSelector = true;
        isHdfsFilesReadyInNextQueue = false;
        List<String> currentNameQueue;
        Map<String , byte[]> currentValueQueue;
        myKafkaProducer = new KafkaProducer<>(producerProp);
               
        while(true){//loop producer logic
            if(queueSelector){
                currentNameQueue = filesNameQueue_One;
                currentValueQueue = filesValueQueue_One;
            }else{
                currentNameQueue = filesNameQueue_Two;
                currentValueQueue = filesValueQueue_Two;
            }
            
            currentQueueLen = currentNameQueue.size();
            //確定queue中有東西才能繼續下去，否則持續調用thread監控filesystem
            if(currentQueueLen == 0){
                boolean currentVoidQueue = queueSelector;
                queueSelector = !queueSelector;
                traceHdfsFileSys.start();
                traceHdfsFileSys.join();
                queueSelector = currentVoidQueue;
                isHdfsFilesReadyInNextQueue = false;
                continue;
            }           
            int callThreadPoint = currentQueueLen * (queueRemainPercent/100);
            for(int i = 0 ; i < currentQueueLen ; i ++){
                if((currentQueueLen - i - 1 <= callThreadPoint) && !isHdfsFilesReadyInNextQueue){
                    traceHdfsFileSys = new Thread(new HdfsFileCapture());
                    traceHdfsFileSys.start();               
                }
                String inFlightName = currentNameQueue.get(i);
                byte[] inFlightValue = currentValueQueue.remove(inFlightName);
                ProducerRecord myRecord = new ProducerRecord(topicName , inFlightName , inFlightValue);
                inFlightQueue.add(inFlightName);
                myKafkaProducer.send(myRecord, new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(e == null){
                            totalNumFiles += 1;
                            System.out.println(totalNumFiles+" ----->"+myRecord.key().toString());
                            inFlightQueue.remove(myRecord.key().toString());
                            destroyQueue.add(myRecord.key().toString());
                        }else{
                            e.printStackTrace();
                        }
                    }              
                });
            }
            //檢查下一次傳送的內容是否準備好了
            if(!isHdfsFilesReadyInNextQueue)
                traceHdfsFileSys.join();     
            currentNameQueue.clear();
            queueSelector = !queueSelector;
            isHdfsFilesReadyInNextQueue = false;
        }
    }
    
    public void sendHdfsFilesPermanentBySingleProducerInAsyn(String topicName , String hdfsDirPath , int pollingFileSystemTimeInMs , 
           int producerBatchSize , int producerRequestBrokerSize , int producerBufferSize) throws InterruptedException, IOException{
        List<String> filesNameQueue_One = new ArrayList<>();
        Map<String , FileStatus> filesValueQueue_One = new HashMap<>();
        List<String> filesNameQueue_Two = new ArrayList<>();
        Map<String , FileStatus> filesValueQueue_Two = new HashMap<>();
        List<String> inFlightQueue = new ArrayList<>();
        List<String> destroyQueue = new ArrayList<>();
        Thread traceHdfsFileSys;
        int currentQueueLen = 0;
        Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , HostIp.KAFKA_SERVER01_IP_PORT);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , KafkaSerialClassName.stringSerializer);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , KafkaSerialClassName.byteArraySerializer);
        producerProp.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProp.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProp.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);//10MB 10242880
        producerProp.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        producerProp.put(ProducerConfig.BUFFER_MEMORY_CONFIG, producerBufferSize);//32MB 33554432
        producerProp.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        producerProp.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, producerRequestBrokerSize); //10242880
        Producer<String , byte[]> myKafkaProducer;
        Hdfs myHdfs = new Hdfs(hdfsDirPath);
        Path hPath = myHdfs.getHdfsPath();
        FileSystem hfs = myHdfs.getHdfsFileSystem().get();
        
    
        if(!hfs.isDirectory(hPath)){
            System.out.println("is not folder path");
            return;
        }
     
        class HdfsFileCapture implements Runnable{
            FileSystem hfs = myHdfs.getHdfsFileSystem().get();
            @Override
            public void run(){
                try{
                    //FileSystem hfs = myHdfs.getHdfsFileSystem().get();
                    if(queueSelector){//表示producer正在用nameQueue_one
                        do{
                            FileStatus[] fileList = hfs.listStatus(hPath);
                            for(FileStatus file : fileList){
                                String fileName = file.getPath().getName();
                                if(fileName.contains("._COPYING_"))
                                    continue;
                                if(!destroyQueue.contains(fileName) && !inFlightQueue.contains(fileName) && !filesNameQueue_One.contains(fileName)){
                                    filesNameQueue_Two.add(fileName);
                                    filesValueQueue_Two.put(fileName , file);
                                }
                            }   
                            if(filesNameQueue_Two.size() == 0){
                                System.out.println("queue 2 checking");
                                Thread.sleep(pollingFileSystemTimeInMs);
                            }else{
                                //hfs.close();
                                isHdfsFilesReadyInNextQueue = true;
                                System.out.println("queue 2 done");
                                break;
                            }                            
                        }while(true);                      
                    }else{//表示rpoducer正在用nameQueue_two
                        do{
                            FileStatus[] fileList = hfs.listStatus(hPath);
                            for(FileStatus file : fileList){
                                String fileName = file.getPath().getName();
                                if(fileName.contains("._COPYING_"))
                                    continue;
                                if(!destroyQueue.contains(fileName) && !inFlightQueue.contains(fileName) && !filesNameQueue_Two.contains(fileName)){
                                    filesNameQueue_One.add(fileName);
                                    filesValueQueue_One.put(fileName , file);
                                }
                            }
                            if(filesNameQueue_One.size() == 0){
                                System.out.println("queue 1 checking");
                                Thread.sleep(pollingFileSystemTimeInMs);
                            }else{
                                //hfs.close();
                                isHdfsFilesReadyInNextQueue = true;
                                System.out.println("queue 1 done");
                                break;
                            }
                        }while(true);                     
                    }                
                }catch(IOException e){
                    e.printStackTrace();
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }
        }
        
        //firstTime, load data to queue one 
        queueSelector = true;
        List<String> currentNameQueue;
        Map<String , FileStatus> currentValueQueue;
        myKafkaProducer = new KafkaProducer<>(producerProp);
        int numPartition = myKafkaProducer.partitionsFor(topicName).size();
        long timeStart =  new Date().getTime();
        while(true){//loop producer logic
            if(queueSelector){
                currentNameQueue = filesNameQueue_One;
                currentValueQueue = filesValueQueue_One;
            }else{
                currentNameQueue = filesNameQueue_Two;
                currentValueQueue = filesValueQueue_Two;
            }
            
            currentQueueLen = currentNameQueue.size();
            //確定queue中有東西才能繼續下去，否則持續調用thread監控filesystem
            if(currentQueueLen == 0){
                boolean currentVoidQueue = queueSelector;
                queueSelector = !queueSelector;
                traceHdfsFileSys = new Thread(new HdfsFileCapture());
                traceHdfsFileSys.start();
                traceHdfsFileSys.join();
                queueSelector = currentVoidQueue;
                isHdfsFilesReadyInNextQueue = false;
                continue;
            }
            //讀取下一回合需要的file list
            traceHdfsFileSys = new Thread(new HdfsFileCapture());
            traceHdfsFileSys.start();
            
            for(int i = 0 ; i < currentQueueLen ; i ++){
                String inFlightName = currentNameQueue.get(i);
                FileStatus file = currentValueQueue.remove(inFlightName);
                long fileSize = hfs.getContentSummary(file.getPath()).getLength();
                long batSize = (long)producerBatchSize;
                inFlightQueue.add(inFlightName);  
                //檔案需切割時
                if(fileSize > batSize){
                    System.out.println("fileSize:" + fileSize + " > batSize:" + batSize);
                    totalNumFiles += 1;
                    int numFragment = (int)Math.ceil(fileSize / (double)(producerBatchSize - 1000));
                    FSDataInputStream inputStream = hfs.open(file.getPath());
                    byte[] fragment = new byte[producerBatchSize - 1000];
                    int next = 1;
                    int noPartition = i % numPartition;
                    System.out.println("ori Remain:" + inputStream.available());
                    //傳送第一片段檔案
                    inputStream.readFully(fragment);
                    ProducerRecord fstBigRecord = new ProducerRecord(topicName , noPartition , "@" + "-" + inFlightName + "-" + "0" + "-" + next , fragment);
                    myKafkaProducer.send(fstBigRecord, new Callback(){
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception e){
                            if(e == null){
                                System.out.println(totalNumFiles + "|" + metadata.topic() + " ----->" + fstBigRecord.key().toString() + " | size: " + fragment.length);
                            }else{
                                e.printStackTrace();
                            }
                        }
                    });
                    //傳送剩下n-2的片段檔案
                    for(int j = 1 ; j < numFragment - 1 ; j++){
                        inputStream.readFully(fragment);
                        next = j + 1;
                        ProducerRecord midBigRecord = new ProducerRecord(topicName , noPartition , "@" + "-" + inFlightName + "-" + j + "-" + next , fragment);
                        myKafkaProducer.send(midBigRecord , new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception e) {
                                if(e == null){
                                    System.out.println(totalNumFiles +"|" + metadata.topic() + " ----->" + midBigRecord.key().toString() + " | size: " + fragment.length);
                                }else{
                                    e.printStackTrace();
                                }
                            }
                        });
                    }
                    //傳送最後一個片段
                    byte[] lastFragment = new byte[inputStream.available()];
                    inputStream.readFully(lastFragment);
                    ProducerRecord lastBigRecord = new ProducerRecord(topicName , noPartition , "@" + "-" + inFlightName + "-" + next + "-" + "~" , lastFragment);
                    myKafkaProducer.send(lastBigRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if(e == null){
                                System.out.println(totalNumFiles +"|" + metadata.topic() + " ----->" + lastBigRecord.key().toString() + "-end" + " | size: " + lastFragment.length);
                                inFlightQueue.remove(lastBigRecord.key().toString());
                                //刪檔案
                                try{
                                    boolean isDelete = hfs.delete(new Path(lastBigRecord.key().toString()), false);
                                    if(isDelete)
                                        destroyQueue.remove(lastBigRecord.key().toString());
                                }catch(IOException ioe){
                                    ioe.printStackTrace();
                                }
                            }else{
                                e.printStackTrace();
                            }
                        }
                    });
                }else{
                    FSDataInputStream inputStream = hfs.open(file.getPath());
                    byte[] inFlightValue = IOUtils.toByteArray(inputStream);
                    ProducerRecord myRecord = new ProducerRecord(topicName , inFlightName , inFlightValue);
                    myKafkaProducer.send(myRecord, new Callback(){
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception e){
                            if(e == null){
                                totalNumFiles += 1;
                                System.out.println(totalNumFiles + " ----->" + myRecord.key().toString());
                                inFlightQueue.remove(myRecord.key().toString());
                                destroyQueue.add(myRecord.key().toString());
                                try{
                                    boolean isDelete = hfs.delete(new Path( hdfsDirPath + myRecord.key().toString()), true);
                                    if(isDelete)
                                        destroyQueue.remove(myRecord.key().toString());
                                }catch(IOException ioe){
                                    ioe.printStackTrace();
                                }
                            }else{
                                e.printStackTrace();
                            }
                        }
                    });
                }

            }
            System.out.println("TimeDiff:" + (new Date().getTime() - timeStart) );
            //檢查下一次傳送的內容是否準備好了
            if(!isHdfsFilesReadyInNextQueue){
                 traceHdfsFileSys.join();
            }                
            currentNameQueue.clear();
            queueSelector = !queueSelector;
            isHdfsFilesReadyInNextQueue = false;
        }    
    }
    
}

