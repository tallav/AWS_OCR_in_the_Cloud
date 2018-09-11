package Manager;

import java.util.HashMap;
import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.InstanceStateName;

public class MonitorWorkers implements Runnable{

	private EC2Class ec2Class;
	private AmazonEC2 ec2;
	private HashMap<String, String> mapIds;
	private Object lock = new Object();
	private boolean shouldStop = false;
	private String WORKER_SCRIPT;
	
	public MonitorWorkers(List<String> workersIds, EC2Class ec2class, String script){
		this.ec2Class = ec2class;
		this.ec2 = ec2class.buildEC2Instance();
		this.mapIds = listToHash(workersIds);
		this.WORKER_SCRIPT = script;
	}
	
	public void setShouldStop(){
		shouldStop = !shouldStop;
	}
	
	private HashMap<String,String> listToHash(List<String> list){
		HashMap<String,String> map = new HashMap<String,String>();
		for(String id : list){
			DescribeInstancesRequest request = new DescribeInstancesRequest().withInstanceIds(id);
			DescribeInstancesResult response = ec2.describeInstances(request);
			String state = response.getReservations().get(0).getInstances().get(0).getState().getName();
			map.put(id, state);
		}
		return map;
	}
	
	public void run(){
		while(!shouldStop){
			synchronized(lock){
				for (String id : mapIds.keySet()) {
					DescribeInstancesRequest request = new DescribeInstancesRequest().withInstanceIds(id);
					DescribeInstancesResult response = ec2.describeInstances(request);
					String state = response.getReservations().get(0).getInstances().get(0).getState().getName();
					//System.out.println("instance id " + id + " is " + state);
					if(!state.equals(InstanceStateName.Running.toString()) &&
							!state.equals(InstanceStateName.Pending.toString())){
						ec2Class.createInstance(WORKER_SCRIPT);
					}
				}
				try {
					Thread.currentThread().sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
}