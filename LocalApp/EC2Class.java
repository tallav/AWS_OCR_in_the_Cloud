package LocalApp;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;

public class EC2Class {
	
	private AmazonEC2 ec2;
    private AWSCredentialsProvider credentialsProvider;
    private String REGION;
    private String AMI;
	
	public EC2Class(AWSCredentialsProvider credentialsProvider, String region, String ami){
		this.credentialsProvider = credentialsProvider;
		this.REGION = region;
		this.AMI = ami;
	}
	
	public AmazonEC2 buildEC2Instance(){
		 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();
		 return ec2;
    }
	
    public boolean isManagerActive() {
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

    public void createManagerInstance(String script) {
        try{
            // Create tag manager
            TagSpecification tag = new TagSpecification()
                    .withResourceType("instance")
                    .withTags(new Tag().withKey("type")
                            .withValue("manager"));
            // Add the manager instance profile
            IamInstanceProfileSpecification profileSpecification = new IamInstanceProfileSpecification()
                    .withArn("arn:aws:iam::968444223449:instance-profile/managerRole");
			//encodes the script of the manager
            String scriptEncoded64 = new String (Base64.encodeBase64(script.toString().getBytes()));			
            // creates and run the manager ec2 instance
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

    public void terminateManagerInstance () {
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
    
    private Instance getManagerInstance() {
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

}