/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.jmx;

//java lib
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Optional;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnectorFactory;
//exception
import java.io.IOException;
import java.net.MalformedURLException;
/**
 *
 * @author 翔翔
 */
public class JmxConnection {
    private static Logger log = LoggerFactory.getLogger(JmxConnection.class);
    private static MBeanServerConnection conn;
    
    
    protected static Optional<MBeanServerConnection> create(String ip , String port){
        String jmxURL = "service:jmx:rmi:///jndi/rmi://" + ip + ":" + port + "/jmxrmi";
        log.info( "init jmx, jmxUrl: {}, and begin to connect it" , jmxURL);
        try{
            JMXServiceURL serviceURL = new JMXServiceURL(jmxURL);
            JMXConnector connector = JMXConnectorFactory.connect(serviceURL,null);
            conn = connector.getMBeanServerConnection();
            if(conn != null)
                return Optional.of(conn);
            else
                return Optional.empty();
        }catch(MalformedURLException e){
            e.printStackTrace();
            return Optional.empty();
        }catch(IOException e){
            e.printStackTrace();
            return Optional.empty();
        }
        
    }
    
    
}
