/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.app;

//kafka lib
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
//java lib
import java.util.concurrent.ExecutionException;
import java.util.Properties;
import java.util.Arrays;
import java.util.Set;



/**
 *
 * @author 工研翔翔哥
 */
public class BrokerOperation {
    
    private AdminClient myAdmin;
    private Properties prop;
    
    public BrokerOperation(String serverIP){
        this.prop = new Properties();
        prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, serverIP);
        this.myAdmin = AdminClient.create(prop);
    }
    
    /*public AdminClient start(){
        this.myAdmin = AdminClient.create(prop);
        return this.myAdmin;
    }*/
    
    public void close(){
        this.myAdmin.close();
    }
    
    public void createTopics(String topicName , int numPartition , short replication) throws ExecutionException , InterruptedException {
        //String name, int numPartitions, short replicationFactor 
        if(numPartition <= 0 )
            numPartition = 1;
        if(replication <= 0)
            replication = 1;
        NewTopic newTopic = new NewTopic(topicName,numPartition,replication); 
        CreateTopicsResult result = myAdmin.createTopics(Arrays.asList(newTopic));
        result.all().get();
    }
    
    public String[] listAllTopics() throws ExecutionException , InterruptedException{
        ListTopicsOptions options = new ListTopicsOptions();
        // includes internal topics such as __consumer_offsets
        options.listInternal(true);
        ListTopicsResult topics = myAdmin.listTopics(options);
        Set<String> topicNames = topics.names().get();
        topicNames.remove("__consumer_offsets");
        String[] dest =  topicNames.toArray(new String[0]);
        return dest;
    }
    
    public Set getAllTopics() throws ExecutionException , InterruptedException{
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult topics = myAdmin.listTopics(options);
        Set<String> topicNames = topics.names().get();
        return topicNames;
    }
    
    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException{
        KafkaFuture<Void> futures = myAdmin.deleteTopics(Arrays.asList(topicName)).all();
        futures.get();
    }
    
}
