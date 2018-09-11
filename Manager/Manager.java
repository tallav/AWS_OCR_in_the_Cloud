package Manager;

import java.util.*;
import java.io.*;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.*;
import org.apache.commons.codec.binary.Base64;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

public class Manager {

	private static SQSClass localappToManagerSQS;
	private static SQSClass tasksSQS;
	private static SQSClass resultsSQS;
	private static EC2Class ec2Class;
	private static AmazonEC2 ec2;
	private static S3Class s3;
	private static AWSCredentialsProvider credentialsProvider;
	private static final String REGION = "us-east-1";
	private static final String AMI = "ami-467ca739";
	private static int n; // number of messages per worker (provided from the user)
	private static int messagesAmount;
	private static int workersAmount;
    private static String tasksQueueName;
    private static String resQueueName;
    private static Thread sendThread;
    private static Thread recieveThread;
    private static Thread monitorThread;
	private static String bucketName;
	private static String localQueueName;
	private static String WORKER_SCRIPT;
	private static String bucketOfJARs;

	public static void main(String[] args) throws Exception {
        n = Integer.parseInt(args[1]);
        localQueueName = args[0];
        bucketOfJARs = args[2];
		//n = 120;
        //localQueueName = "LocalappToManager0ceef";
        initialize();
        
        startManager();
        messagesAmount = getMessagesAmount();

        // the manager creates workers ec2 instances
        createWorkerInstances(WORKER_SCRIPT);
     
        ArrayList<String> workersIds = ec2Class.getWorkerInstancesIds();
        MonitorWorkers monitorTask = new MonitorWorkers(workersIds, ec2Class, WORKER_SCRIPT);
        ManagerSendTask sendTask = new ManagerSendTask(tasksSQS, s3, bucketName, messagesAmount);
        ManagerRecieveTask recieveTask = new ManagerRecieveTask(resultsSQS, messagesAmount);
        monitorThread = new Thread(monitorTask);
        sendThread = new Thread(sendTask);
        recieveThread = new Thread(recieveTask);
        monitorThread.start();
        sendThread.start();
        recieveThread.start();
        
        sendThread.join();
        recieveThread.join();
        monitorTask.setShouldStop();
		
        System.out.println("------------------------------------------------------------------");
        
        HashMap map = recieveTask.getMappedResults();
        System.out.println(map.size());
        createAndUploadOutput(map);
        localappToManagerSQS.sendMessage("done");

        terminate();
    }

    private static void initialize(){
        credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
        s3 = new S3Class(credentialsProvider, REGION);
        s3.createS3();
        localappToManagerSQS = new SQSClass(credentialsProvider, REGION, localQueueName);
        localappToManagerSQS.startSQSConnection();
        ec2Class = new EC2Class(credentialsProvider, REGION, AMI);
        ec2 = ec2Class.buildEC2Instance();
    }

    public static void startManager() {
        // checks if the input was uploaded to s3
        boolean received = false;
        Message receivedMessage = null;
        bucketName = null;
        while(!received){
            System.out.println("Waiting for input message");
            receivedMessage = localappToManagerSQS.receiveMessage();
            if (receivedMessage != null) {
                try{
                    receivedMessage.acknowledge();
                    // the recived message is the bucket name that contains the input file
                    bucketName = ((TextMessage)receivedMessage).getText();
                    received = true;
                } catch (JMSException e) {
                    System.out.println("Recived message acknoledge failed: " + e);
                }
            }
        }
        tasksQueueName = "tasksQueue" + UUID.randomUUID().toString().substring(0, 5);
        tasksSQS = new SQSClass(credentialsProvider, REGION, tasksQueueName);
        tasksSQS.createSqsQueue();
        tasksSQS.startSQSConnection();
        
        resQueueName =  "resQueue" + UUID.randomUUID().toString().substring(0, 5);
        resultsSQS = new SQSClass(credentialsProvider, REGION, resQueueName);
        resultsSQS.createSqsQueue();
        resultsSQS.startSQSConnection();
        
        WORKER_SCRIPT = "#!/bin/bash\n"
                + "aws configure set aws_access_key_id " + credentialsProvider.getCredentials().getAWSAccessKeyId() + "\n"
                + "aws configure set aws_secret_access_key " + credentialsProvider.getCredentials().getAWSSecretKey() + "\n"
                + "mkdir ass1\n"
                + "aws s3 cp s3://" + bucketOfJARs + "/Worker.jar ass1/Worker.jar\n"
                + "yes | sudo yum install java-1.8.0\n"
                + "yes | sudo yum remove java-1.7.0-openjdk\n"
                + "sudo java -jar ass1/Worker.jar " + tasksQueueName + " " + resQueueName + "\n";
    }

    public static int getMessagesAmount(){
        //s3.saveObjectFromS3ToLocalPath(bucketName, "input.txt", "C:\\Users\\Tal\\workspace\\LocalApp\\input.txt");
        s3.saveObjectFromS3ToLocalPath(bucketName, "input.txt", "ass1/input.txt");
        LineNumberReader ln = null;
        try {
        	//ln = new LineNumberReader(new FileReader("C:\\Users\\Tal\\workspace\\LocalApp\\input.txt"));
            ln = new LineNumberReader(new FileReader("ass1/input.txt"));
            ln.skip(Long.MAX_VALUE);
        } catch (FileNotFoundException e) {
            System.out.println("getMessagesAmount error: " + e);
        } catch (IOException e) {
            System.out.println("getMessagesAmount error: " + e);
        }
        return ln.getLineNumber()+1;
    }

	public static void createWorkerInstances(String WORKER_SCRIPT) {
		// calculate the amount of workers needed
		workersAmount = messagesAmount/n;
		// in case messagesAmount is not divisible by n
		if(workersAmount*n < messagesAmount){
			workersAmount = workersAmount + 1;
		}
		System.out.println("messagesAmount: " + messagesAmount);
		System.out.println("workersAmount: " + workersAmount);
		try{
			// Create tag worker
			TagSpecification tag = new TagSpecification()
					.withResourceType("instance")
					.withTags(new Tag().withKey("type")
					.withValue("worker"));
			// Add the worker instance profile
			IamInstanceProfileSpecification profileSpecification = new IamInstanceProfileSpecification()
					//.withArn("arn:aws:iam::526469203882:instance-profile/workerRole");
					.withArn("arn:aws:iam::968444223449:instance-profile/workerRole");
			//encodes the worker launch script (user data)
            String scriptEncoded64 = new String(Base64.encodeBase64(WORKER_SCRIPT.toString().getBytes()));
			RunInstancesRequest request = new RunInstancesRequest()
					.withImageId(AMI)
					.withInstanceType(InstanceType.T2Micro)
					.withMaxCount(workersAmount)
					.withMinCount(workersAmount)
					.withTagSpecifications(tag)
					.withIamInstanceProfile(profileSpecification)
					.withKeyName("keypair2")
					.withUserData(scriptEncoded64);
			ec2.runInstances(request);
			System.out.println("Created " + workersAmount + " EC2 instances");
		}
		catch (Exception e) {
			System.out.println("Failed to create workers instances: " + e);
		}
	}

	private static void createAndUploadOutput(HashMap map){
		StringBuilder sb = new StringBuilder();
		FileWriter fstream = null;
		try {
			//fstream = new FileWriter("C:\\Users\\Tal\\workspace\\LocalApp\\output.txt");
			fstream = new FileWriter("ass1/output.txt");
			BufferedWriter out = new BufferedWriter(fstream);
			for (int i=0; i<map.size();i++){
				List<String> msgs = parseMsg(map.get(i).toString());
				sb.append("URL:");
				sb.append("\n");
				sb.append(msgs.get(0));
				sb.append("\n");
				sb.append("RES:");
				sb.append("\n");
				sb.append(msgs.get(1));
				sb.append("\n");
			}
			out.write(sb.toString());
			out.close();
		
			//s3.uploadToS3ByFileName("C:\\Users\\Tal\\workspace\\LocalApp","output.txt",bucketName);
			s3.uploadToS3ByFileName("ass1","output.txt",bucketName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static List<String> parseMsg(String msgToParse){
		int index = msgToParse.indexOf("Result: ");
		String url = msgToParse.substring(0,index);
		String res = msgToParse.substring(index+8);
		List<String> result = new ArrayList<String>();
		result.add(url);
		result.add(res);
		return result;
	}

    private static void terminate(){
	    ec2Class.terminateWorkerInstances(); 
        tasksSQS.closeSQSConnection();
        tasksSQS.deleteQueue();
        resultsSQS.closeSQSConnection();
        resultsSQS.deleteQueue();
        localappToManagerSQS.closeSQSConnection();
        sendThread.interrupt();
        recieveThread.interrupt();
        monitorThread.interrupt();
    }
    
}