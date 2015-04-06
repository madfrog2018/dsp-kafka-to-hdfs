package com.pxene.kafka.consumer;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class StreamConsumer implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(StreamConsumer
            .class);
    private KafkaStream<byte[], byte[]> stream;
    private static Calendar calendar;
    private static final DateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
//    private static final DateFormat sdf2 = new SimpleDateFormat("HHmm");
    private static final DateFormat sdf3 = new SimpleDateFormat("yyyyMMddHHmm");
    private static final String WRAP_SYMBOL = System.getProperty(
            "line.separator", "\n");
    private static Config config;
    private FileSystem fileSystem ;
    private FSDataOutputStream outputStream;
    private int flushBatchSize = 0;
    private int flushSize = 1000;
    private String rootStr;
    private String currentDir;
    private volatile String nowDate = getDate();
    private volatile String nowTime = getTime();
    public StreamConsumer(String rootStr, KafkaStream<byte[], byte[]> stream,
                          Config config) {
        this.stream = stream;
        this.rootStr = rootStr;
        this.config = config;
    }

    public void run() {
        for (MessageAndMetadata<byte[], byte[]> aStream : stream) {
            write(new String(aStream.message()));
        }
    }
    public void write(String message) {
        try {
            possiblyRotateFile();
            outputStream.write((message + WRAP_SYMBOL).getBytes());
            flushBatchSize++;
            flushWritesIfNeeded();
        } catch (Exception e) {
            close();
            LOGGER.error("******write error : " + e);
        }
    }



    /**
     * ��ȡ��ʽ��������Ϊ�ļ�����
     * @return
     */
    private String getDate() {
        calendar = Calendar.getInstance();
        return sdf1.format(calendar.getTime());
    }

    /**
     * ��ȡ��ʽ��ʱ����Ϊ�ļ���
     * @return
     */
    private String getTime() {
        calendar = Calendar.getInstance();
        return sdf3.format(calendar.getTime());
    }

    private void createIfNotExists(Path path) {
        try {
            if (!fileSystem.exists(path)) {
                fileSystem.mkdirs(path);
            }
        } catch (IOException e) {
            LOGGER.error("******create dir error : " + e);
        }
    }

    private void deleteIfExists(Path path) {
        try {
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
            }
        } catch (IOException e) {
            LOGGER.error("******deleteIfExists error : " + e);
        }
    }



    private void possiblyRotateFile() {
        try {
            String intervalDate = getDate();
            LOGGER.info("nowDate is " + nowDate + ", intervalDate is " + intervalDate);
            long interval = sdf1.parse(intervalDate).getTime() - sdf1.parse
                    (nowDate).getTime();
            if (interval >= 86400000) {
                nowDate = intervalDate;
                LOGGER.info("modify the nowDate");
                //隔天后，关闭当前的outputStream。
                outputStream.hflush();
                outputStream.close();
                //修改currentTime为隔天的时间
                nowTime = getTime();
            }
            currentDir = rootStr + "/" + nowDate ;
//            LOGGER.info("current dir is " + currentDir);
            Path hdfsCurrentDir = new Path(currentDir);
            fileSystem = hdfsCurrentDir.getFileSystem(config.configure());
            createIfNotExists(hdfsCurrentDir);
//            LOGGER.info("create current dir");
            rotateFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void rotateFile() throws ParseException {
        try {
//            LOGGER.info("rotate file start");
            String intervalTime = sdf3.format(new Date().getTime());
            LOGGER.info("nowTime is " + nowTime + ", intervalTime is "
                    + intervalTime);
            long interval = sdf3.parse(intervalTime).getTime() - sdf3.parse
                    (nowTime).getTime();
            if (interval >= 30*60*1000) {
                LOGGER.info("modify now time");
                nowTime = intervalTime;
                outputStream.hflush();
                outputStream.close();
            }
            Path rotatedPath = new Path(currentDir + "/" + nowTime);
//            LOGGER.info("path is " + rotatedPath);
            if (!fileSystem.exists(rotatedPath)){
                outputStream = fileSystem.create(rotatedPath);
//                LOGGER.info("create the path");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


//    /**
//     * �ж��Ƿ����
//     * @return
//     */
//    private boolean isDateEquals() {
//        String newDate = getDate();
//        if (newDate.equals(lastDate)) {
//            return true;
//        }
//        lastDate = newDate;
//        return false;
//    }

    private void flushWritesIfNeeded() {
        try {
            if (flushBatchSize == flushSize) {
                outputStream.hflush();
                flushBatchSize = 0;
            }
        } catch (IOException e) {
            LOGGER.error("******flushWritesIfNeeded error : " + e);
        }
    }

    public void close() {
        try {
            outputStream.hflush();
            outputStream.close();
            fileSystem.close();
        } catch (IOException e) {
            LOGGER.error("******close error : " + e);
        }
    }

    public static void main(String[] args) throws ParseException, InterruptedException {

        System.out.println( sdf1.parse("20150404").getTime() - sdf1.parse
                ("20150403").getTime());

        calendar = Calendar.getInstance();
        String nowTime =  sdf3.format(calendar.getTime());


        Thread.sleep(2000);
        String intervalTime = sdf3.format(new Date().getTime());
        LOGGER.info("nowTime is " + nowTime + ", intervalTime is "
                + intervalTime);
        long interval = sdf3.parse("201504032305").getTime() - sdf3.parse
                ("201504032255").getTime();
        System.out.println("interval is " + interval);
        System.out.println("" + (interval == 10*60*1000));
    }
}
