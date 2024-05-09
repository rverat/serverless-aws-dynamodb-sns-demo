package com.serverless;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.serverless.domain.User;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

import java.util.Map;

public class Handler implements RequestHandler<Map<String, Object>, String> {

	@Override
	public String handleRequest(Map<String, Object> input, Context context) {

		LambdaLogger logger = context.getLogger();

		logger.log("received: {}" + input); //Simple Map imput test

		Region region = Region.US_EAST_2;
		DynamoDbClient ddb = DynamoDbClient.builder()
				.region(region)
				.build();

		// Create a DynamoDbEnhancedClient and use the DynamoDbClient object.
		DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
				.dynamoDbClient(ddb)
				.build();

		// Create a DynamoDbTable object based on Employee.
		DynamoDbTable<User> table = enhancedClient.table("users", TableSchema.fromBean(User.class));

		SnsClient snsClient = SnsClient.builder()
				.region(Region.US_EAST_2)
				.build();

		String topicArn = "arn:aws:sns:us-east-2:767397962761:sns-users-topic";


		try {
			logger.log("Start fetching users from DynamoDB");
			SdkIterable<User> results = table.scan().items();
			logger.log("Finish fetching users from DynamoDB");


			results.forEach(user -> {
				logger.log("Send email to: " + user.getName());
			});

			String emailAddress = "user.getEmail()";
			String message = prepareEmailMessage("");

			pubTopic(snsClient, message, emailAddress, topicArn, logger);

			snsClient.close();
			ddb.close();
			logger.log("Finish all");

			return "OPERATION SUCCES"; //Simple string return

		} catch (Exception e) {
			logger.log("Error fetching users from DynamoDB" + e.getCause());
			return "ERROR: Unable to fetch users from DynamoDB";
		}
	}

	public void pubTopic(SnsClient snsClient, String message, String email, String topicArn, LambdaLogger logger) {

		try {
			PublishRequest request = PublishRequest.builder()
					.message(message)
					.topicArn(topicArn)
					.subject("Important Notification of the Day")
					.build();

			PublishResponse result = snsClient.publish(request);

			logger.log(result.messageId() + " Message sent. Status is " + result.sdkHttpResponse().statusCode());

		} catch (SnsException e) {
			logger.log(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
	}

	private String prepareEmailMessage(String string) {
		return "Dear " + string + ",\n\n" +
				" Today is for create, solver and share it";
	}
}
