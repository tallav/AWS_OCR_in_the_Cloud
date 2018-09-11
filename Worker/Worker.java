package Worker;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.asprise.ocr.Ocr;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;

public class Worker {

    private static SQSClass resQueue;
    private static SQSClass tasksQueue;
    private static SQSClass workerQueue;
    private static Ocr ocr;
    private static AWSCredentialsProvider credentialsProvider;
    private static final String REGION = "us-east-1";

    public static void main(String[] args) throws Exception {
        init(args[0],args[1]);
        
        Ocr.setUp(); // one time setup
        ocr = new Ocr();
        ocr.startEngine("eng", Ocr.SPEED_FAST);

        work();

        Terminate();
    }

    private static void init(String tasksQueueName, String resQueueName){
        credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
        tasksQueue = new SQSClass(credentialsProvider, REGION, tasksQueueName);
        resQueue = new SQSClass(credentialsProvider, REGION, resQueueName);;
        tasksQueue.startSQSConnection();
        resQueue.startSQSConnection();
        /*a queue to count worker messages
        String workerQueueName = "workerQueue" + UUID.randomUUID().toString().substring(0, 5);
        workerQueue = new SQSClass(credentialsProvider, REGION, workerQueueName);
        workerQueue.createSqsQueue();
        workerQueue.startSQSConnection();
        */
    }

    protected static void work(){
        while (true){
            javax.jms.Message msg = tasksQueue.receiveMessage();

            if (msg == null)
                return; // the queue of tasks is empty. worker is done.
            else {
                try {
                	msg.acknowledge();
                    String textMsg = ((TextMessage)msg).getText();
                    String imageType = downloadImg(textMsg);
                    String res = workOnMsgTypeUrl(textMsg,imageType);
                    resQueue.sendMessage(res);
                    //workerQueue.sendMessage(res);
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //returns imageType
    private static String downloadImg(String imgURL){
        URL url = null;
        String imageType = "";
        try {
            url = new URL(imgURL);
            InputStream in = new BufferedInputStream(url.openStream());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            int n = 0;
            while (-1!=(n=in.read(buf)))
            {
                out.write(buf, 0, n);
            }
            out.close();
            in.close();
            byte[] response = out.toByteArray();
            imageType =imgURL.substring(imgURL.lastIndexOf('.') + 1) ;
            FileOutputStream fos = new FileOutputStream("image."+imageType);
            fos.write(response);
            fos.close();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return imageType;
    }

    private static String workOnMsgTypeUrl(String msg,String imgType)  {
        String res = ocr.recognize(new File[] {new File("image."+imgType)},
                Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT);
        //System.out.println("URL: " + msg + " Result: " + res);
        return msg + "Result: " + res;
    }

    private static void Terminate(){
        ocr.stopEngine();
        tasksQueue.closeSQSConnection();
        resQueue.closeSQSConnection();
    }
}