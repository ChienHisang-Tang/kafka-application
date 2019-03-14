/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.connect;

//dslab
import com.dslab.kafka.para.HostIp;
//java
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;

//hadoop
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 *
 * @author 工研翔翔
 */
public class Hdfs {
    
    private String hdfsPathStr;
    private String hdfsUrl;
    private Configuration hdfsConf;
    private String nameNodeHostIp = HostIp.HADOOP_SERVER01_IP;
    private String hadoopHomeDir = "/opt/cloudera/parcels/CDH";//para
    private String userName = "hdfs";//para
    
    public Hdfs(String hdfsPath){
        this.hdfsConf = new Configuration();
        this.hdfsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        this.hdfsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        System.setProperty("HADOOP_USER_NAME", this.userName);
        System.setProperty("hadoop.home.dir", this.hadoopHomeDir);
        this.hdfsUrl = "hdfs://" + HostIp.HADOOP_NAMENODE_IP_PORT + "/" + hdfsPath;
        this.hdfsConf.set("fs.defaultFS", this.hdfsUrl);
        this.hdfsPathStr = hdfsPath;
    }
    

    public Optional<FileSystem> getHdfsFileSystem(){
        FileSystem fs = null;
        URI uri = URI.create(this.hdfsUrl);
        try{
            fs = FileSystem.get(uri, this.hdfsConf);
        }catch(IOException e){
            e.printStackTrace();
        }
        
        return Optional.ofNullable(fs);
    }
    
    public Path getHdfsPath(){
        Path path = new Path(this.hdfsPathStr);
        return path;
    }
    
   
    public Optional<byte[]> getFile(String hdfsPath) throws IOException{ 
        String hdfsUrl = "hdfs://" + HostIp.HADOOP_NAMENODE_IP_PORT + "/" + hdfsPath;
        URI uri = URI.create(hdfsUrl);
        Path hdfsReadPath = new Path(hdfsPath);            
        this.hdfsConf.set("fs.defaultFS", hdfsUrl);
        byte[] out = null; 
        FileSystem fs = FileSystem.get(uri, hdfsConf);
        
        if(fs.isFile(hdfsReadPath)){
            FSDataInputStream inputStream = fs.open(hdfsReadPath);
            //out = IOUtils.toString(inputStream,"UTF-8");
            out = IOUtils.toByteArray(inputStream);
            inputStream.close();
            fs.close();
        }       
        return Optional.ofNullable(out);
    }
    
    public void putFile(String hdfsPath , byte[] inputData) throws IOException{
        String hdfsUrl = "hdfs://" + HostIp.HADOOP_NAMENODE_IP_PORT +  hdfsPath;
        URI uri = URI.create(hdfsUrl);
        Path hdfsWritePath = new Path(hdfsPath);
        this.hdfsConf.set("fs.defaultFS", hdfsUrl);
        FileSystem fs = FileSystem.get(uri, this.hdfsConf);
         
        FSDataOutputStream outputStream=fs.create(hdfsWritePath);
        outputStream.write(inputData);
        outputStream.close();
    }
       
}
