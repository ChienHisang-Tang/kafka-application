/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.app;
import java.util.ArrayList;
/**
 *
 * @author 工研翔翔哥
 */
public class TopicList {
    
    public enum Topic{
        TOPIC_ONE , 
        TOPIC_TWO;
        
        
        @Override
         public String toString(){
            switch(this){
                case TOPIC_ONE: return "bm-p1-r2";
                case TOPIC_TWO: return "byAPI";
                default:    return null;
            }
        }
         
    }
    
    public static ArrayList<String> getAllTopic(){
       ArrayList<String> arrayList = new ArrayList<>();
       for(Topic t : Topic.values()){
           arrayList.add(t.toString());
       }
       return arrayList;
    }
}
