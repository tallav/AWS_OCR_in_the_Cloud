package LocalApp;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.codec.binary.Base64;
import javax.jms.Message;
import javax.jms.TextMessage;

public class LocalAppMain {

    private final static String REGION = "us-east-1";
    private final static String AMI = "ami-467ca739";
    private static AWSCredentialsProvider credentialsProvider;
    private static String queueName;
    private static AmazonEC2 ec2;
    private static EC2Class ec2Class;
    private static S3Class s3class;
    private static SQSClass localappToManagerSQS;
    private static String bucketName;
    private static boolean shouldTerminate;
    private static String MANAGER_SCRIPT;
    private static Integer n;
    private static String inputFileName;
    private static String bucketOfJARs = "shirantal";

    public static void main(String[] args) throws Exception {
    	inputFileName = args[0];
    	n = Integer.parseInt(args[1]);
    	initialize();
    	//upload input file to the bucket
    	String path1 = System.getProperty("user.home") + "/Desktop";
    	s3class.uploadToS3ByFileName(path1, inputFileName, bucketName);
        // sends a message to the menager with the bucket name
        localappToManagerSQS.sendMessage(bucketName);
        
       if (!isManagerActive())
        	createManagerInstance(MANAGER_SCRIPT);
	   else
	      	runManagerInstance(MANAGER_SCRIPT);
       
       Thread.sleep(240000);
        Message receivedMessage = null;
        while (!shouldTerminate) { // wait for msg from manager which indicates that he is done
            System.out.println("Waiting for done message");
            receivedMessage = localappToManagerSQS.receiveMessage();
            if (receivedMessage != null) {
                if (((TextMessage)receivedMessage).getText().equals("done")){
                    shouldTerminate = true;
                    receivedMessage.acknowledge();
                }
            }
        }
        if (shouldTerminate){
        	createHtml();
            terminate();
        }
    }

    private static void initialize() {
    	credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
    	shouldTerminate = false;

        s3class = new S3Class(credentialsProvider, REGION);
        s3class.createS3();
        bucketName = "ass1bucket" + UUID.randomUUID().toString().substring(0, 5);
        s3class.createS3Bucket(bucketName);

        ec2Class = new EC2Class(credentialsProvider, REGION, AMI);
        ec2 = ec2Class.buildEC2Instance();

        queueName = "LocalappToManager" + UUID.randomUUID().toString().substring(0, 5);
        localappToManagerSQS = new SQSClass(credentialsProvider, REGION, queueName);
        localappToManagerSQS.createSqsQueue();
        localappToManagerSQS.startSQSConnection();

        MANAGER_SCRIPT =
                "#!/bin/bash -x\n"
        		+ "aws configure set aws_access_key_id " + credentialsProvider.getCredentials().getAWSAccessKeyId() + "\n"
        		+ "aws configure set aws_secret_access_key " + credentialsProvider.getCredentials().getAWSSecretKey() + "\n"
                + "mkdir ass1\n"
        		+ "aws s3 cp s3://"+bucketOfJARs+"/Manager.jar ass1/Manager.jar\n"
        		+ "yes | sudo yum install java-1.8.0\n"
        		+ "yes | sudo yum remove java-1.7.0-openjdk\n" 
        		+ "sudo java -jar ass1/Manager.jar " + queueName + " " + n + " " + bucketOfJARs +"\n"; 
    }

    public static boolean isManagerActive() {
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        DescribeInstancesResult response = ec2.describeInstances(request);
        boolean isActive = false;
        boolean done = false;
        while(!done) {
            // List all instances and check if there is a running instance with manager tag
            for (Reservation reservation : response.getReservations()) {
                for (Instance instance : reservation.getInstances()) {
                    for (Tag tag : instance.getTags()) {
                        if (tag.getKey().equals("type") &&
                                tag.getValue().equals("manager") &&
                                !instance.getState().getName().equals(InstanceStateName.Terminated.toString()) &&
                                !instance.getState().getName().equals(InstanceStateName.ShuttingDown.toString())) {
                            isActive = instance.getState().getName().equals(InstanceStateName.Running.toString());
                        }
                    }
                }
            }
            request.setNextToken(response.getNextToken());
            if(response.getNextToken() == null) {
                done = true;
            }
        }
        return isActive;
    }

    public static Instance getManagerInstance() {
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        DescribeInstancesResult response = ec2.describeInstances(request);
        Instance managerInst = null;
        boolean done = false;
        while(!done) {
            // List all instances and check if there is a running instance with manager tag
            for (Reservation reservation : response.getReservations()) {
                for (Instance instance : reservation.getInstances()) {
                    for (Tag tag : instance.getTags()) {
                        if (tag.getKey().equals("type") &&
                                tag.getValue().equals("manager") &&
                                !instance.getState().getName().equals(InstanceStateName.Terminated.toString()) &&
                                !instance.getState().getName().equals(InstanceStateName.ShuttingDown.toString())) {
                            managerInst = instance;
                        }
                    }
                }
            }
            request.setNextToken(response.getNextToken());
            if(response.getNextToken() == null) {
                done = true;
            }
        }
        return managerInst;
    }

    private static void runManagerInstance(String script) {
        Instance instnace = getManagerInstance();
        String scriptEncoded64 = new String(Base64.encodeBase64(script.toString().getBytes()));
        ec2.stopInstances(new StopInstancesRequest());
        ec2.startInstances(new StartInstancesRequest().withAdditionalInfo(scriptEncoded64).withInstanceIds(instnace.getInstanceId()));
        System.out.println("Running an exists EC2 instance " + instnace.getInstanceId());
    }

    private static void createManagerInstance(String script) {
        try{
            // Create tag manager
            TagSpecification tag = new TagSpecification()
                    .withResourceType("instance")
                    .withTags(new Tag().withKey("type")
                            .withValue("manager"));
            // Add the manager instance profile
            IamInstanceProfileSpecification profileSpecification = new IamInstanceProfileSpecification()
            		//.withArn("arn:aws:iam::526469203882:instance-profile/ManagerRole");
                    .withArn("arn:aws:iam::968444223449:instance-profile/managerRole");
			// encode the manager script
			String scriptEncoded64 = new String(Base64.encodeBase64(script.toString().getBytes()));//Base64.getEncoder().encodeToString(script.getBytes("UTF-8"));
			// launch the manager
            RunInstancesRequest request = new RunInstancesRequest()
                    .withImageId(AMI)
                    .withInstanceType(InstanceType.T2Micro)
                    .withMaxCount(1)
                    .withMinCount(1)
                    .withTagSpecifications(tag)
                    .withIamInstanceProfile(profileSpecification)
                    .withKeyName("keypair2")
                    .withUserData(scriptEncoded64);
            ec2.runInstances(request);
            System.out.println("Created manager EC2 instance");
        }
        catch (Exception e) {
            System.out.println("Failed to create manager instance: " + e);
        }
    }

    private static void terminate(){
    	localappToManagerSQS.closeSQSConnection();
        localappToManagerSQS.deleteQueue();
        terminateManagerInstance();
    }

    private static void terminateManagerInstance () {
        try {
            TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest()
                    .withInstanceIds(getManagerInstance().getInstanceId());
            ec2.terminateInstances(terminateRequest);
            System.out.println("Terminated manager EC2 instance");
        }
        catch (AmazonServiceException e) {
            System.out.println("Failed to terminate manager instance: " + e);
        }
    }

    private static void createHtml(){
        try {
            // Create file
            StringBuilder sb = new StringBuilder();
            FileWriter fstream = new FileWriter("out.html");
            BufferedWriter out = new BufferedWriter(fstream);
            sb.append("\n" +
                    "<html>\n" +
                    "<title>OCR</title>\n" +
                    "<body>\n");
            sb.append(createHtmlHelper("output.txt"));
            sb.append("</body>\n" +
                    "<html>");
            out.write(sb.toString());
            //Close the output stream
            out.close();
        } catch (Exception e) {//Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
    }

    private static String createHtmlHelper(String fileName){
        StringBuilder sb = new StringBuilder();
        S3Object input = (S3Object) s3class.downloadFromS3(fileName, bucketName);
        S3ObjectInputStream stream = ((S3Object)input).getObjectContent();
        List<String> msgs = splitInputStream(stream);
        for (int i=0; i<msgs.size();i++){
            //List<String> msg = parseMsg(msgs.get(i));
            if (msgs.get(i).equals("URL:")){
                sb.append("\t<p>\n");
                sb.append("\t\t<img src=");
                sb.append("\"");
                sb.append(msgs.get(i+1));
                sb.append("\"><br/>");
                int j=i+3;
                while (j<msgs.size() && !msgs.get(j).equals("URL:")){
                    sb.append(msgs.get(j)+"\n");
                    j++;
                }
                sb.append("\t</p>\n");
                if (j<msgs.size())
                    i=j-1;
            }
        }
        return sb.toString();
    }
    
    // reads the input file and devide every line to a url (string)
    private static List<String> splitInputStream(InputStream input) {
        List<String> lines = new LinkedList<String>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = null;
            try {
                line = reader.readLine();
            } catch (IOException e) {
                System.out.println("Faild to read line. io exception: " + e);
            }
            if (line == null) break;
            lines.add(line);
        }
        return lines;
    }
    
}