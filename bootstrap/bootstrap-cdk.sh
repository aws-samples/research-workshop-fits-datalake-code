#!/bin/sh
# bootstrap cdk - https://docs.aws.amazon.com/cdk/latest/guide/bootstrapping.html
# assumes that the aws cli is configured - https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html

ACCOUNT=$(aws sts get-caller-identity --query 'Account' --output text)
REGION=$(aws configure get region)      # current region from ~/.aws/credentials on Cloud9 instance

cdk bootstrap aws://${ACCOUNT}/${REGION}
