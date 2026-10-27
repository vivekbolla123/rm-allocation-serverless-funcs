#! /bin/bash
set -e
#Variable definition section
Lmd_Func="prod-rm-lambda-rm-pricing-triggers"
ECR_Repo="prod-rm-ecr-rm-pricing-triggers"
ECR_Domain="471112573018.dkr.ecr.ap-south-1.amazonaws.com"
Local_Img_Tag="rm-pricing-triggers-image"
ECR_Img_Tag="latest"
ECR_Image_URL=$ECR_Domain/$ECR_Repo:$ECR_Img_Tag
AWS_Region="ap-south-1"
SSM_ENV="prod"
#Execution section
clear
echo ""
echo "###################################################################"
echo "[[[  Running: git fetch --all on current working directory: ]]]"
echo "$PWD"
git fetch --all
echo "###################################################################"  && sleep 2
echo ""
echo ""
echo "###################################################################"
echo "[[[  Running git checkout on $1  ]]]"
git checkout $1
GIT_COMMIT_SHA=$(git rev-parse --short HEAD)
GIT_BRANCH=$(git branch --show-current)
GIT_TAG=$(git describe --tags --always 2>/dev/null || echo "$GIT_COMMIT_SHA")
echo "###################################################################" && sleep 2
echo ""
echo ""
echo "###################################################################"
echo "[[[  Git status O/P: ]]]"
Git_Status=`git status|head -n 5`
echo "$Git_Status"
echo "###################################################################" && sleep 2
echo ""
echo ""
read -p "Do you want to proceed? (yes/no) " User_Input
case $User_Input in
        yes ) echo Proceeding with deployment;
              aws ecr get-login-password --region $AWS_Region | docker login --username AWS --password-stdin $ECR_Image_URL
              # Fetch DD_API_KEY from SSM and set it on the Lambda
              DD_API_KEY=$(aws ssm get-parameter --name "/copilot/qp/${SSM_ENV}/secrets/DD_API_KEY" --with-decryption --query "Parameter.Value" --output text --region $AWS_Region)
              EXISTING_VARS=$(aws lambda get-function-configuration --region $AWS_Region --function-name $Lmd_Func --query "Environment.Variables" --output json 2>/dev/null || echo "{}")
              UPDATED_VARS=$(echo "$EXISTING_VARS" | DD_API_KEY="$DD_API_KEY" python3 -c 'import os,sys,json; d=json.load(sys.stdin) or {}; d["DD_API_KEY"]=os.environ["DD_API_KEY"]; print(json.dumps({"Variables": d}))')
              aws lambda update-function-configuration --region $AWS_Region --function-name $Lmd_Func --environment "$UPDATED_VARS" > /dev/null
              echo "DD_API_KEY environment variable set on [$Lmd_Func]"
              cp -r ../rm-allocation-configurations/configsettings.py ./configsettings.py
              cp -r ../rm-allocation-configurations/Constants.py ./Constants.py
              cp -r ../rm-allocation-configurations/constants_utils.py ./constants_utils.py
              cp -r ../rm-allocation-configurations/common_utils.py ./common_utils.py
              docker image build --no-cache -t $Local_Img_Tag --build-arg ENVN=prod --build-arg GIT_COMMIT_SHA=$GIT_COMMIT_SHA --build-arg GIT_BRANCH=$GIT_BRANCH --build-arg GIT_TAG=$GIT_TAG .
              docker tag $Local_Img_Tag $ECR_Image_URL
              docker push $ECR_Image_URL
              echo "Waiting for function configuration update to complete..."
              aws lambda wait function-updated --region $AWS_Region --function-name $Lmd_Func
              aws lambda update-function-code --region $AWS_Region --function-name $Lmd_Func --image-uri $ECR_Image_URL
              echo ""
              echo ""
              echo "Lambda Function [$Lmd_Func] current state: "
              aws lambda get-function --region $AWS_Region --function-name $Lmd_Func | egrep -e "State" -e "LastModified"
              echo ""
              echo ""
              rm -f ./configsettings.py
              rm -f ./Constants.py
              rm -f ./constants_utils.py
              rm -f ./common_utils.py
              break;;
        no ) echo Exiting...;
              exit;;
        * ) echo Invalid input;;
esac