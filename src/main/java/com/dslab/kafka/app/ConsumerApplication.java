/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.app;

//dslab
import com.dslab.kafka.para.HostIp;
import com.dslab.kafka.para.KafkaSerialClassName;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
//kafka lib
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
//java lib
import java.util.Properties;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 *
 * @author 工研翔翔哥
 */
public class ConsumerApplication {
   
    public void getFilesPermanentBySingleConsumer(String topicName){
        //相關buffer
        Map<String,byte[]> relatedBuffer = new HashMap<>();
        //不相關buffer
        Map<String,byte[]> startBuffer = new HashMap<>();
        Map<String,byte[]> commonBuffer = new HashMap<>();
        Map<String,byte[]> midBuffer = new HashMap<>();
        //寫入路徑
        String folderPath = "/opt/kafkaCode/data/";
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HostIp.KAFKA_SERVER01_IP_PORT);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaSerialClassName.stringDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSerialClassName.byteArrayDeserializer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG , "group01");
        
        KafkaConsumer<String , byte[]> myKafkaConsumer = new KafkaConsumer(props);
        myKafkaConsumer.subscribe(Arrays.asList(topicName));
       
        final int fileNamePos = 1;
        final int fileIndexPos = 2;
        final int fileNextPos = 3;
        final String splitToken = "-";
        FileOutputStream outWriter = null;
        
        //搞定好資料夾
        File file = new File(folderPath);
        if(!file.exists()){
            if(!file.mkdir()){
                System.out.println(folderPath + "created fail");
                return;
            } 
        }
        
        try{
            boolean hasStartFileWriting = false;
            String writingFileName = "";
            String writingFileNext = "";
            while(true){
                ConsumerRecords<String , byte[]> records = myKafkaConsumer.poll(1000);
                //取出資料
                for(ConsumerRecord<String , byte[]> record : records){
                    String key = record.key();
                    //判斷是否為普通檔案
                    if(key.contains("@")){
                        String[] splitArray = key.split(splitToken);
                        String fileName = splitArray[fileNamePos];
                        String fileIndex = splitArray[fileIndexPos];
                        String fileNext = splitArray[fileNextPos];
                        //判斷是start還是midd
                        if(Integer.valueOf(fileIndex) == 0){
                        //start
                            if(hasStartFileWriting){
                                startBuffer.put(fileName, record.value());
                                continue;
                            }else{
                                writingFileName = fileName;
                                writingFileNext = fileNext;
                            }
                        }else{
                        //middd 
                            if(hasStartFileWriting){                                   
                                if(fileName.equals(writingFileName) && !fileIndex.equals(writingFileNext)){
                                    relatedBuffer.put(fileIndex, record.value());
                                    continue;
                                }else if(!fileName.equals(writingFileName)){
                                    midBuffer.put(key, record.value());
                                    continue;
                                }
                                //若符合當先需要的片段，則繼續向下執行。
                            }else{
                                midBuffer.put(key, record.value());
                                continue;
                            }
                        }
                    }else{
                        //判斷為普通檔案，若沒有startfile正在寫入則直接寫入目標目錄，若有則放進buffer中
                        String fileName = key;
                        if(hasStartFileWriting){
                            commonBuffer.put(fileName , record.value());
                        }else{                           
                            File commonFile = new File(folderPath + fileName);
                            commonFile.createNewFile();
                            FileOutputStream fout = new FileOutputStream(commonFile);
                            fout.write(record.value());
                            fout.close();   
                        }
                        continue;
                    }
                    
                    if(!hasStartFileWriting){
                        //第一次開始寫
                        hasStartFileWriting = true;
                        File startFile = new File(folderPath + writingFileName);
                        if(startFile.exists()){
                            System.out.println("[ System Msg ] : " + "File name : " + writingFileName + " repeat , " + "original file will be replace.");
                            startFile.delete();
                        }
                        startFile.createNewFile();
                        outWriter = new FileOutputStream(startFile , true);
                        outWriter.write(record.value());
                        System.out.println("writed: " + record.key());
                        //開始找midbuffer中是否有比start先到的mid片段
                        if(!midBuffer.isEmpty()){
                            Set<String> sets = midBuffer.keySet();
                            List<String> lists = new ArrayList<>();
                            for(String keyName : sets){
                                String[] keyNameMsg = keyName.split(splitToken);
                                if(writingFileName.equals(keyNameMsg[fileNamePos])){
                                    //insert sort
                                    if(lists.isEmpty()){
                                        lists.add(keyName); 
                                    }else{
                                        int listsCompareIndex = lists.size() - 1;   //start from tail
                                        int keyNameMsgIndex = Integer.valueOf(keyNameMsg[fileIndexPos]);
                                        while(Integer.valueOf(lists.get(listsCompareIndex).split(splitToken)[fileIndexPos]) > keyNameMsgIndex){
                                            listsCompareIndex--;
                                            if(listsCompareIndex == -1)
                                                break;
                                        }
                                        lists.add(listsCompareIndex + 1 , keyName);
                                    }
                                }
                            }
                            //找找看裡面有沒有第2片段，有就開始link write，沒有就放入relatedbuffer
                            if(!lists.isEmpty()){                                   
                                int fragIndex = Integer.valueOf(lists.get(0).split(splitToken)[fileIndexPos]);
                                if(fragIndex == 1){
                                    int remainIndex = -1;
                                    File myFile = new File(folderPath + writingFileName);
                                    outWriter = new FileOutputStream(myFile , true);
                                    for(int i = 0 ; i < lists.size() ; i++){
                                        String keyNameInList = lists.get(i);
                                        String[] keyNameInListMsg = keyNameInList.split(splitToken);
                                        String curFragIndex = keyNameInListMsg[fileIndexPos];
                                        String nextFragIndex = keyNameInListMsg[fileNextPos];
                                        if(writingFileNext.equals(curFragIndex)){
                                            outWriter.write(midBuffer.remove(keyNameInList));
                                            System.out.println("writed: " + keyNameInList);
                                            writingFileNext = nextFragIndex;
                                            remainIndex = i;
                                        }else{
                                            break;
                                        }  
                                    }
                                    outWriter.close();
                                    List<String> remainList = new ArrayList<>();
                                    for(int i = remainIndex + 1 ; i < lists.size() ; i++){
                                        remainList.add( lists.get(i) );
                                    }
                                    lists.clear();
                                    //檢查是否有殘餘的檔案，全放入relatedBuffer
                                    if(!remainList.isEmpty()){
                                        for(String remainKey : remainList){
                                            relatedBuffer.put(remainKey , midBuffer.remove(remainKey));
                                        }
                                    }
                                }else{
                                    for(String keyNameInList : lists){                                          
                                        relatedBuffer.put(keyNameInList , midBuffer.remove(keyNameInList));
                                    }
                                    lists.clear();
                                }
                            }       
                        }
                        
                    }else{
                        //正在寫當中
                        File myFile = new File(folderPath + writingFileName);
                        outWriter = new FileOutputStream(myFile , true);
                        outWriter.write(record.value());
                        System.out.println("[ System Msg ] : " + "W---> " + record.key());
                        //更新next值
                        String curKey = record.key();
                        writingFileNext = curKey.split(splitToken)[fileNextPos];
                        //寫完之後，檢查relatedBuffer是否有可以接下去的元素
                        if(!relatedBuffer.isEmpty()){
                            boolean isGetMatch = false;
                            while(!isGetMatch){
                                Set<String> reSet = relatedBuffer.keySet();
                                isGetMatch = false;
                                for(String keyInReSet : reSet){
                                    String[] msg = keyInReSet.split(splitToken);
                                    String index = msg[fileIndexPos];
                                    if(index.equals(writingFileNext)){
                                        outWriter.write(relatedBuffer.remove(keyInReSet));
                                        System.out.println("[ System Msg ] : " + "W---> " + keyInReSet);
                                        writingFileNext = msg[fileNextPos];
                                        isGetMatch = true;
                                    }
                                }
                                if(relatedBuffer.isEmpty())
                                    break;
                            }
                        }
                    }
                    //檢查是否next是~
                    if(writingFileNext.equals("~")){
                        outWriter.close();
                        System.out.println("[ System Msg ] : " + writingFileName + " done.");
                        writingFileNext = "";
                        writingFileName = "";
                        hasStartFileWriting = false;
                        System.out.println("[ System Msg ] : " + "StartBuffer: " + startBuffer.size() + " | " + "RelatedBuffer" + relatedBuffer.size() +
                                " | " + "CommonBuffer: " + commonBuffer.size() + " | " + "midBuffer: " + midBuffer.size());
                    }else{
                        System.out.println("[ System Msg ] : " + "Writing File : " + writingFileName + " , were get new record to done this file.");
                        System.out.println("[ System Msg ] : " + "StartBuffer: " + startBuffer.size() + " | " + "RelatedBuffer" + relatedBuffer.size() +
                                " | " + "CommonBuffer: " + commonBuffer.size() + " | " + "midBuffer: " + midBuffer.size());
                        continue;
                    }
                    //檢查commonBuffer是否有人
                    if(!commonBuffer.isEmpty()){
                        Set<String> fileNames = commonBuffer.keySet();
                        for(String fileName : fileNames){
                            File myFile = new File(folderPath +  fileName);
                            if(myFile.exists()){
                                System.out.println("[ System Msg ] : " + "File name : " + fileName + " repeat , " + "original file will be replace.");
                                myFile.delete();
                            }
                            myFile.createNewFile();
                            outWriter = new FileOutputStream(myFile);
                            outWriter.write(commonBuffer.remove(fileName));
                            outWriter.close();
                        }
                    }
                    //檢查startBuffer中是否有人
                    if(!startBuffer.isEmpty()){                      
                        List<String> startFileNames = new ArrayList<>();
                        for(String startKey : startBuffer.keySet())
                            startFileNames.add(startKey);
                        for(String startFileName : startFileNames){
                            hasStartFileWriting = true;
                            writingFileName = startFileName;
                            writingFileNext = "1";
                            File startFile = new File(folderPath + writingFileName);
                            //檢查檔案是否重複
                            if(startFile.exists()){
                                System.out.println("[ System Msg ] : " + "File name : " + writingFileName + " repeat , " + "original file will be replace.");
                                startFile.delete();
                            }
                            startFile.createNewFile();
                            outWriter = new FileOutputStream(startFile , true);
                            outWriter.write(startBuffer.remove(writingFileName));
                            System.out.println("[ System Msg ] : " + "StartBuffer: " + startBuffer.size() + " | " + "RelatedBuffer" + relatedBuffer.size() +
                                " | " + "CommonBuffer: " + commonBuffer.size() + " | " + "midBuffer: " + midBuffer.size());
                            if(!midBuffer.isEmpty()){
                                Set<String> sets = midBuffer.keySet();
                                List<String> lists = new ArrayList<>();
                                for(String keyName : sets){
                                    String[] keyNameMsg = keyName.split(splitToken);
                                    if(writingFileName.equals(keyNameMsg[fileNamePos])){
                                        //insert sort
                                        if(lists.isEmpty()){
                                            lists.add(keyName); 
                                        }else{
                                            int listsCompareIndex = lists.size() - 1;   //start from tail
                                            int keyNameMsgIndex = Integer.valueOf(keyNameMsg[fileIndexPos]);
                                            while(Integer.valueOf(lists.get(listsCompareIndex).split(splitToken)[fileIndexPos]) > keyNameMsgIndex){
                                                listsCompareIndex--;
                                                if(listsCompareIndex == -1)
                                                    break;
                                            }
                                            lists.add(listsCompareIndex + 1, keyName);
                                        }
                                    }
                                }
                                //找找看裡面有沒有第2片段，有就開始link write，沒有就放入relatedbuffers
                                if(!lists.isEmpty()){                                   
                                    int fragIndex = Integer.valueOf(lists.get(0).split(splitToken)[fileIndexPos]);
                                    if(fragIndex == 1){
                                        int remainIndex = -1;    //紀錄目前用到哪邊
                                        File myFile = new File(folderPath + writingFileName);
                                        outWriter = new FileOutputStream(myFile , true);
                                        for(int i = 0 ; i < lists.size() ; i++){
                                            String keyNameInList = lists.get(i);
                                            String[] keyNameInListMsg = keyNameInList.split(splitToken);
                                            String curFragIndex = keyNameInListMsg[fileIndexPos];
                                            String nextFragIndex = keyNameInListMsg[fileNextPos];
                                            if(writingFileNext.equals(curFragIndex)){
                                                outWriter.write(midBuffer.remove(keyNameInList));
                                                System.out.println("[ System Msg ] : " + "W-->" + keyNameInList);
                                                System.out.println("[ System Msg ] : " + "StartBuffer: " + startBuffer.size() + " | " + "RelatedBuffer" + relatedBuffer.size() +
                                                " | " + "CommonBuffer: " + commonBuffer.size() + " | " + "midBuffer: " + midBuffer.size());
                                                writingFileNext = nextFragIndex;
                                                remainIndex = i;
                                            }else{
                                                break;
                                            }  
                                        }
                                        outWriter.close();
                                        List<String> remainList = new ArrayList<>();
                                        for(int i = remainIndex + 1 ; i < lists.size() ; i++){
                                            remainList.add( lists.get(i) );
                                        }
                                        lists.clear();
                                        //檢查是否有殘餘的檔案，全放入relatedBuffer
                                        if(!remainList.isEmpty()){
                                            System.out.println("[ System Msg ] : " + "Remain msg in midList.");
                                            System.out.println("[ System Msg ] : " + "Move remain msg to relatedBuffer.");
                                            for(String remainKey : remainList){
                                                relatedBuffer.put(remainKey , midBuffer.remove(remainKey));
                                                System.out.println("[ System Msg ] : " + "StartBuffer: " + startBuffer.size() + " | " + "RelatedBuffer" + relatedBuffer.size() +
                                                " | " + "CommonBuffer: " + commonBuffer.size() + " | " + "midBuffer: " + midBuffer.size());
                                            }
                                        }
                                    }else{
                                        System.out.println("[ System Msg ] : " + "Cant find " + writingFileName + " 's Fragment2.");
                                        System.out.println("[ System Msg ] : " + "Move midList msg to relatedBuffer.");
                                        for(String keyNameInList : lists){                                          
                                            relatedBuffer.put(keyNameInList , midBuffer.remove(keyNameInList));
                                        }
                                        lists.clear();
                                    }
                                }       
                            }
                            //判斷是否寫完了
                            if(writingFileNext.equals("~")){
                                outWriter.close();
                                System.out.println("[ System Msg ] : " + writingFileName + " done.");
                                writingFileNext = "";
                                writingFileName = "";
                                hasStartFileWriting = false;
                                continue;
                            }else{
                                //離開startbuffer迴圈，從record那邊獲得資料。
                                System.out.println("[ System Msg ] : " + "Writing File : " + writingFileName + " , were get new record to done this file.");
                                System.out.println("[ System Msg ] : " + "Wirting at: " + record.key() + "|" + "StartBuffer: " + startBuffer.size() + 
                                " | " + "CommonBuffer: " + commonBuffer.size() + " | " + "midBuffer: " + midBuffer.size());
                                break;
                            }
                        }
                    }
                }
            }
        }catch(FileNotFoundException fne){
            fne.printStackTrace();
        }catch(IOException ioe){
            ioe.printStackTrace();
        }finally{
            myKafkaConsumer.close();
        }
    }
      
}
