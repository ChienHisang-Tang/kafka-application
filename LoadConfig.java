/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.app;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author 唐健翔
 */
public class LoadConfig {
    private static String configFilePath = "./akConfig.properties";
    private static Properties props;
    
    public static Properties init(){
        props = new Properties();
        try{
            props.load(new FileInputStream(configFilePath));
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
       
        return LoadConfig.props;
    }
    
    public static void setConfigFilePath(String path){
        configFilePath = path;
    }
    
}
