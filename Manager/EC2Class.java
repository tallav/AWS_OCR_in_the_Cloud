package Manager;

import java.util.ArrayList;

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
	
	public void terminateWorkerInstances () {
		try {
			ArrayList<String> workerIds = getWorkerInstancesIds();
			System.out.println(workerIds);
			TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest()
					.withInstanceIds(workerIds);
			ec2.terminateInstances(terminateRequest);
			System.out.println("Terminated worker EC2 instances");
		}
		catch (AmazonServiceException e) {
			System.out.println("Failed to terminate worker instances: " + e);
		}
	}
	
	public ArrayList<String> getWorkerInstancesIds() {
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		DescribeInstancesResult response = ec2.describeInstances(request);
		ArrayList<String> workersInstancesIds= new ArrayList<String>();
		boolean done = false;
		while(!done) {
			// List all instances and count the running workers
			for(Reservation reservation : response.getReservations()) {
				for(Instance instance : reservation.getInstances()) {
					for(Tag tag : instance.getTags()) {
						if (tag.getKey().equals("type") &&
								tag.getValue().equals("worker") &&
								!instance.getState().getName().equals(InstanceStateName.Terminated.toString()) &&
								!instance.getState().getName().equals(InstanceStateName.ShuttingDown.toString())) {
							workersInstancesIds.add(instance.getInstanceId());
						}
					}
				}
			}
			request.setNextToken(response.getNextToken());
			if(response.getNextToken() == null) {
				done = true;
			}
		}
		return workersInstancesIds;
	}
	
	public void createInstance(String script) {
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
            String scriptEncoded64 = new String(Base64.encodeBase64(script.toString().getBytes()));//Base64.getEncoder().encodeToString(script.getBytes("UTF-8"));
			RunInstancesRequest request = new RunInstancesRequest()
					.withImageId(AMI)
					.withInstanceType(InstanceType.T2Micro)
					.withMaxCount(1)
					.withMinCount(1)
					.withTagSpecifications(tag)
                    .withUserData(scriptEncoded64)
					.withIamInstanceProfile(profileSpecification);
			ec2.runInstances(request);
			System.out.println("Created EC2 instance");
		}
		catch (Exception e) {
			System.out.println("Failed to create workers instances: " + e);
		}
	}

}