import json, boto3
from botocore.exceptions import ClientError
from typing import List, Optional, Dict

iam_client = boto3.client('iam')


def get_trust_policy_for_user(user_name:str, account_id:str=None):
    """
    Return trust relationship policy can be used to assume this role by an IAM user of another AWS account
    """
    if account_id is None:
        account_id = boto3.client('sts').get_caller_identity().get('Account')

    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{account_id}:user/{user_name}"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }


def get_trust_policy_for_service(service_name: str="ec2.amazonaws.com"):
    """
    Return trust relationship policy can be used to assume this role by a particular service in the same account.
    """
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": service_name
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    

def create_custom_role(role_name:str, assume_role:Dict, policy_arn_list:List[str], permissions_boundary:Optional[str]=None):
    """
    Create a role and attach policies to the role. Skip creation if role already exists.
    """
    # Create role
    try:
        create_role_res = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role),
            Description='Role for stepfunctions to execute sagemaker jobs',
            Tags=[
                {
                    'Key': 'Owner',
                    'Value': 'qinjie'
                }
            ],
            PermissionsBoundary=permissions_boundary
        )
    except ClientError as error:
        if error.response['Error']['Code'] == 'EntityAlreadyExists':
            print('Role already exists... skipping...')
        else:
            print('Unexpected error occurred... Role could not be created', str(error))
            raise
    
    # Attach policy
    for arn in policy_arn_list:
        response = iam_client.attach_role_policy(RoleName=role_name, PolicyArn=arn)

    response = iam_client.get_role(RoleName=role_name)
    return response


def lambda_handler(event, context):
    TRUST_TYPE_USER = "user"
    
    DEFAULT_TRUST_SERVICE = "states.amazonaws.com"
    DEFAULT_ROLE_NAME = "u-StepFunctionsExecutionWithSageMakerTask"
    DEFAULT_POLICY_ARN_LIST = [
        "arn:aws:iam::305326993135:policy/u-stepfunction-sagemaker-trainingjob", 
        "arn:aws:iam::305326993135:policy/u-stepfunction-sagemaker-transformjob"
    ]
    DEFAULT_PERMISSIONS_BOUNDARY = "arn:aws:iam::305326993135:policy/GCCIAccountBoundary"

    
    trust_service = event.get("trust_service", DEFAULT_TRUST_SERVICE)
    role_name = event.get("role_name", DEFAULT_ROLE_NAME)
    policy_arn_list = event.get("policy_arn_list", DEFAULT_POLICY_ARN_LIST)
    permissions_boundary = event.get("permissions_boundary", DEFAULT_PERMISSIONS_BOUNDARY)
    
    trust_type = event.get("trust_type", None)
    user_name = event.get("user_name")
    account_id = event.get("account_id")
    
    if trust_type == TRUST_TYPE_USER:
        assume_role = get_trust_policy_for_service(user_name, account_id)
    else:
        assume_role = get_trust_policy_for_service(trust_service)
    
    resp = create_custom_role(role_name, assume_role, policy_arn_list, permissions_boundary)
    print(resp)
    
    return json.loads(json.dumps(resp, default=str))
