package Manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class ManagerSendTask implements Runnable{

	private SQSClass tasksSQS;
	private S3Class s3;
	private String bucketName;
	private int messagesAmount;
	
	public ManagerSendTask(SQSClass tasksSQS, S3Class s3, String bucketName, int messagesAmount){
		this.tasksSQS = tasksSQS;
		this.s3 = s3;
		this.bucketName = bucketName;
		this.messagesAmount = messagesAmount;
	}
	
	public void run() {
		tasksSQS.startSQSConnection();
		createMsgs("input.txt", bucketName);
	}
	
	// sends messages to the sqs, every message contains one url from the input
	private void createMsgs(String fileName, String bucketName){
		S3Object input = (S3Object) s3.downloadFromS3(fileName, bucketName);
		S3ObjectInputStream stream = ((S3Object)input).getObjectContent();
		List<String> lines = new LinkedList<String>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        while(true) {
			String line = null;
			try {
				line = reader.readLine();
				if (line!=null){
					if (!line.equals("") || !line.equals("\n")) {
						tasksSQS.sendMessage(line);
						System.out.println("thread 2 added msg num: " + messagesAmount + " " + line);
						messagesAmount--;
					}
				}
			} catch (IOException e) {
				System.out.println("could not read line. io exception: " + e);
			}
			if (line == null) break;
            lines.add(line);
        }
	}

}