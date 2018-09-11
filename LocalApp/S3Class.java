package LocalApp;

import java.io.File;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

public class S3Class {

	private AmazonS3 s3;
    private AWSCredentialsProvider credentialsProvider;
    private String REGION;
	
	public S3Class(AWSCredentialsProvider credentialsProvider, String region){
		this.credentialsProvider = credentialsProvider;
		this.REGION = region;
	}
	
	public AmazonS3 createS3(){
		s3 = AmazonS3ClientBuilder.standard()
               .withCredentials(credentialsProvider)
               .withRegion(REGION)
               .build();
		return s3;
	}

    public void createS3Bucket(String bucketName){
	    System.out.println("Creating bucket: " + bucketName);
	    if (!s3.doesBucketExistV2(bucketName))
            s3.createBucket(bucketName.toLowerCase());
        //List the buckets in your account
        System.out.println("Listing buckets:");
        for (Bucket bucket : s3.listBuckets()) {
            System.out.println(" - " + bucket.getName());
        }
   }

    public void uploadToS3(String directoryName, String bucketName){
       System.out.println("Uploading a new directory to S3");
       File dir = new File(directoryName);
       for (File file : dir.listFiles()) {
           String key = file.getName().replace('\\', '_').replace('/', '_').replace(':', '_');
           PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
           s3.putObject(req);
       }
   }

    public void uploadToS3ByFileName(String directoryName,String fileName, String bucketName){
        System.out.println("Uploading a new file to S3");
        File dir = new File(directoryName);
        for (File file : dir.listFiles()) {
            String key = file.getName().replace('\\', '_').replace('/', '_').replace(':', '_');
           if (file.getName().equals(fileName)){
               PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
               s3.putObject(req);
           }
        }
    }

    public Object downloadFromS3(String fileName, String bucketName){
        System.out.println("Downloading an object from s3");
        String key = fileName.replace('\\', '_').replace('/','_').replace(':', '_');
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        return object;
    }

    public ObjectListing getObjectsFromBucket(String bucketName){
       System.out.println("Listing objects:");
       ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
               .withBucketName(bucketName));
       for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries())
           System.out.println(" - " + objectSummary.getKey() + "  " +
                   "(size = " + objectSummary.getSize() + ")");
        return objectListing;
   }

    public void removeBucket(String bucketName){
       System.out.println("Deleting objects from bucket");
       if (!s3.doesBucketExistV2(bucketName)) // validate that bucket exist
           return;
       ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
               .withBucketName(bucketName));
       for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries())
           s3.deleteObject(bucketName, objectSummary.getKey());
       System.out.println("Deleting bucket " + bucketName);
       s3.deleteBucket(bucketName);
   }

   public void saveObjectFromS3ToLocalPath(String bucketName,String fileKey,String destPath){
	   File destination = new File(destPath);
       ObjectMetadata object = s3.getObject(new GetObjectRequest(bucketName, fileKey),destination);
       System.out.println("File saved to: " + destPath);
   }
   
}