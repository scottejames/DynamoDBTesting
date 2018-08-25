import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestMain {
    public static void createTable(){
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);
        String tableName = "Test";
        DynamoDBMapper mapper = new DynamoDBMapper(client);

        CreateTableRequest req = mapper.generateCreateTableRequest(TestModel.class);
        // Table provision throughput is still required since it cannot be specified in your POJO
        req.setProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        // Fire off the CreateTableRequest using the low-level client
        client.createTable(req);


    }

    public static void createData(){
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);
        String tableName = "Test";
        DynamoDBMapper mapper = new DynamoDBMapper(client);

        TestModel model = new TestModel();
        model.setPersonName("Fred");
        model.setOwner("scott");
        model.setAge(12);
        mapper.save(model);

        TestModel modelTwo = new TestModel();
        modelTwo.setPersonName("Fred");
        modelTwo.setOwner("scott");

        modelTwo.setAge(16);
        mapper.save(modelTwo);

        TestModel modelThree = new TestModel();
        modelThree.setPersonName("Fred");
        modelThree.setOwner("anna");

        modelThree.setAge(18);
        mapper.save(modelThree);
    }

    public static void queryData(){
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);
        String tableName = "Test";
        DynamoDBMapper mapper = new DynamoDBMapper(client);

        System.out.println("Getting object with id 1c6c6efc-5c63-4139-aef6-94780ed65a18");

        TestModel results = mapper.load(TestModel.class,"1c6c6efc-5c63-4139-aef6-94780ed65a18");
        System.out.println("Retrived " + results.getPersonName() + " age " + results.getAge());

        //mapper.delete(results);
    }

    public static void queryDataLIST(){
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);
        String tableName = "Test";
        DynamoDBMapper mapper = new DynamoDBMapper(client);

        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":v1", new AttributeValue().withS("scott"));
        DynamoDBQueryExpression<TestModel> queryExpression = new DynamoDBQueryExpression<TestModel>()
                .withKeyConditionExpression("owningUser = :v1")
                .withExpressionAttributeValues(eav);

        List<TestModel> freds =  mapper.query(TestModel.class,queryExpression);

        for (TestModel m : freds){
            System.out.println("Name " + m.getPersonName());
            System.out.println("Age " + m.getAge());
        }
    }

    public static void scanData(){
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);
        String tableName = "Test";
        DynamoDBMapper mapper = new DynamoDBMapper(client);

        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":v1", new AttributeValue().withS("Fred"));
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withFilterExpression("personName = :v1")
                .withExpressionAttributeValues(eav);
        List<TestModel> freds =  mapper.scan(TestModel.class,scanExpression);

        for (TestModel m : freds){
            System.out.println("Name " + m.getPersonName());
            System.out.println("Age " + m.getAge());
            System.out.println("Owner " + m.getOwner());
        }
    }

    public static void main(String [] args){
        scanData();



    }
}
