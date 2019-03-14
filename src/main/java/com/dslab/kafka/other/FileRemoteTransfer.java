/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.other;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;





/**
 *
 * @author 工研翔翔哥
 */
public class FileRemoteTransfer {
    public void scpReciveFile(String destHostIp , String destHostUser , String remotePath , String localPath , String fileName){
        List<String> cmd = new ArrayList<>();
        cmd.add("scp");
        cmd.add(destHostUser + "@" + destHostIp + ":" + remotePath + fileName);
        cmd.add(localPath);
        
        ProcessBuilder processBuilder = new ProcessBuilder(cmd);
        try{
             Process scpProcess = processBuilder.start();
             int exitCode = scpProcess.waitFor();
             System.out.println("exitCode = "+exitCode);
        }catch(IOException e){
            e.printStackTrace();
        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }
    
    public void scpSendFile(String destHostIp , String destHostUser , String remotePath , String localPath , String fileName){
        List<String> cmd = new ArrayList<>();
        cmd.add("scp");
        cmd.add(localPath + fileName);
        cmd.add(destHostUser + "@" + destHostIp + ":" + remotePath);
        
        ProcessBuilder processBuilder = new ProcessBuilder(cmd);
        try{
             Process scpProcess = processBuilder.start();
             int exitCode = scpProcess.waitFor();
             System.out.println("exitCode = "+exitCode);
        }catch(IOException e){
            e.printStackTrace();
        }catch(InterruptedException e){
            e.printStackTrace();
        }       
    }
    
    public void scpGetFolder(String destHostIp , String destHostUser , String remotePath , String localPath){
        List<String> cmd = new ArrayList<>();
        cmd.add("scp");
        cmd.add("-r");
        cmd.add(destHostUser + "@" + destHostIp + ":" + remotePath);
        cmd.add(localPath);
        
        ProcessBuilder processBuilder = new ProcessBuilder(cmd);
        try{
             Process scpProcess = processBuilder.start();
             int exitCode = scpProcess.waitFor();
             System.out.println("exitCode = "+exitCode);
        }catch(IOException e){
            e.printStackTrace();
        }catch(InterruptedException e){
            e.printStackTrace();
        }      
    }
    
    public void scpSendFolder(String destHostIp , String destHostUser , String remotePath , String localPath){
         List<String> cmd = new ArrayList<>();
        cmd.add("scp");
        cmd.add("-r");
        cmd.add(localPath);
        cmd.add(destHostUser + "@" + destHostIp + ":" + remotePath);
        
        ProcessBuilder processBuilder = new ProcessBuilder(cmd);
        try{
             Process scpProcess = processBuilder.start();
             int exitCode = scpProcess.waitFor();
             System.out.println("exitCode = "+exitCode);
        }catch(IOException e){
            e.printStackTrace();
        }catch(InterruptedException e){
            e.printStackTrace();
        }   
    }
    
    public void getFileFromHds(){
        List<String> cmd = new ArrayList<>();
        cmd.add("curl");
        cmd.add("http://192.168.103.173:8000/dataservice/v1/access?from=hdfs:///opt/file.txt&to=file:///opt/xfolder/");
         ProcessBuilder processBuilder = new ProcessBuilder(cmd);
        try{
             Process scpProcess = processBuilder.start();
             int exitCode = scpProcess.waitFor();
             System.out.println("exitCode = "+exitCode);
        }catch(IOException e){
            e.printStackTrace();
        }catch(InterruptedException e){
            e.printStackTrace();
        }       
    }
}
