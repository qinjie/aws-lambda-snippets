# Create IAM Role using Lambda Function

This lambda function creates an IAM Role and attach list of policies. 

For trust relationship, it can either be an AWS service, or an user from another AWS account.

* By default, trust relationship for an AWS service. 

  ```json
  {
  	"Effect": "Allow",
  	"Principal": {
  		"Service": "states.amazonaws.com"
  	},
  	"Action": "sts:AssumeRole"
  }
  ```
  
* If the trust relationship is for an AWS user, set `trust_type` to `user`.

  ```json
  {
  	"Effect": "Allow",
  	"Principal": {
  		"AWS": f"arn:aws:iam::{account_id}:user/{user_name}"
  	},
  	"Action": "sts:AssumeRole"
  }
  ```

## Input Param `event` 

Following are the attributes in the `event` parameter.

* When `trust_type` is `user`, provide `user_name` and/or `account_id` values. When `account_id` is missing, it will use same AWS account as the lambda function. 
* Otherwise, provide `trust_service` value, which should be a proper AWS service principal value. For example, `states.amazonaws.com` for Step Functions.

#### Sample Event JSON for AWS Service as Trust Relationship

```
{
  "trust_service": "states.amazonaws.com",
  "role_name": "u-StepFunctionsExecutionWithSageMakerTask",
  "policy_arn_list": [
      "arn:aws:iam::305326993135:policy/u-stepfunction-sagemaker-trainingjob", 
      "arn:aws:iam::305326993135:policy/u-stepfunction-sagemaker-transformjob"
  ],
  "permissions_boundary": "arn:aws:iam::305326993135:policy/GCCIAccountBoundary"
}
```

#### Sample Event JSON for User as Trust Relationship

```
{
  "role_name": "u-StepFunctionsExecutionWithSageMakerTask",
  "policy_arn_list": [
      "arn:aws:iam::305326993135:policy/u-stepfunction-sagemaker-trainingjob", 
      "arn:aws:iam::305326993135:policy/u-stepfunction-sagemaker-transformjob"
  ],
  "permissions_boundary": "arn:aws:iam::305326993135:policy/GCCIAccountBoundary",
  "trust_type": "service",
  "user_name": "user1",
  "account_id": "123456789"
}
```

