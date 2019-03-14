/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.other;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 *
 * @author 工研翔翔
 */
public class FileCreator {
    
    private final int ByteUnit = 1;
    private final int KByteUnit = 1024;
    private final int MByteUnit = 1024 * 1024;
    private final int GByteUnit = 1024 * 1024 * 1024;
    private String fileContentPerLine = "D";
    private int sizePerLine = 1;
    private final int sizeNewLineSign = 2;
    private int totalSizePerLine = 3;
    
    public void setFileContentPerLine(String str){
        this.fileContentPerLine = str;
        this.sizePerLine = this.fileContentPerLine.length();
        this.totalSizePerLine = this.sizePerLine + this.sizeNewLineSign;
    }
        
    public void createFile(String dir , int fileSize , char unit)throws IOException {
        float round = 0;
        long unitRound = 0;
        int size = fileSize;
                              
        switch(unit){
            case 'b':
                unitRound = this.ByteUnit;
                break;
            case 'k':
                unitRound = this.KByteUnit;
                break;
            case 'm':
                unitRound = this.MByteUnit;
                break;
            case 'g':
                unitRound = this.GByteUnit;
                break;                
            default:
                System.out.println("unit not support");
                break;           
        }
        
        if(unitRound == 0){
            return;
        }else{
            round = Math.round(unitRound * size / this.totalSizePerLine);
        }
        
        File myFile = new File(dir);
        if(!myFile.exists()){
           myFile.getParentFile().mkdirs();
           try{
               myFile.createNewFile();
           }catch(IOException e){
               e.printStackTrace();
           }
        }
        
        FileOutputStream fout = new FileOutputStream(myFile);
        BufferedWriter bufferWriter = new BufferedWriter(new OutputStreamWriter(fout));
        
        for(int i = 0 ; i < round ; i++){
            bufferWriter.write(this.fileContentPerLine);
            bufferWriter.newLine();
        }
        bufferWriter.flush();        
        bufferWriter.close();
    }
    
    public void createFiles(String folderPath , int totalSize , char totalSizeUnit , int fileSize , char fileSizeUnit){
        System.out.println("start");
        int numFiles = 0;
        float roundFile = 0;
        long unitRoundTotal = 0;
        long unitRoundFile = 0;
        
        switch(totalSizeUnit){
            case 'b':
                unitRoundTotal = this.ByteUnit;
                break;
            case 'k':
                unitRoundTotal = this.KByteUnit;
                break;
            case 'm':
                unitRoundTotal = this.MByteUnit;
                break;
            case 'g':
                unitRoundTotal = this.GByteUnit;
                break;                
            default:
                System.out.println("unit not support");
                return;           
        }
        switch(fileSizeUnit){
            case 'b':
                unitRoundFile = this.ByteUnit;
                break;
            case 'k':
                unitRoundFile = this.KByteUnit;
                break;
            case 'm':
                unitRoundFile = this.MByteUnit;
                break;
            case 'g':
                unitRoundFile = this.GByteUnit;
                break;                
            default:
                System.out.println("unit not support");
                return;          
        }

        numFiles = Math.round((totalSize * unitRoundTotal) / (fileSize * unitRoundFile));
        if(numFiles == 0)
            numFiles = 1;
        
        roundFile = Math.round(fileSize * unitRoundFile / this.totalSizePerLine);
        
        File myFolder = new File(folderPath);
        if(!myFolder.exists()){
           myFolder.mkdirs();
        }
        System.out.println("writing...");
        for(int i = 0 ; i < numFiles ; i++){
            File myFile = new File(folderPath+"/"+"newFile"+i);
            try{
                myFile.createNewFile();
            }catch(IOException e){
                e.printStackTrace();
            }
            try{
                FileOutputStream fout = new FileOutputStream(myFile);
                BufferedWriter bufferWriter = new BufferedWriter(new OutputStreamWriter(fout));
                for(int j = 0 ; j < roundFile ; j++){
                    bufferWriter.write(this.fileContentPerLine);
                    bufferWriter.newLine();
                }
                bufferWriter.flush();
                bufferWriter.close();
            }catch(FileNotFoundException e){
                e.printStackTrace();
            }catch(IOException e){
                e.printStackTrace();
            }
        }
        System.out.println("complete");
              
    }
    
    private void createFiles(String folder , int numFiles , int fileSize , char fileSizeUnit){
    
    }
        
}
