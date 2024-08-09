#! /bin/bash
set -e
#Variable definition section
Lmd_Func="rm-allocation-serverless-funcs_rm-allocation-process-inputs"
ECR_Repo="$Lmd_Func" #If ECR repo name and Lambda function name are both same
ECR_Domain="471112573018.dkr.ecr.ap-south-1.amazonaws.com"
Local_Img_Tag="rm-allocation-process-inputs-image"
ECR_Img_Tag="latest"
ECR_Image_URL=$ECR_Domain/$ECR_Repo:$ECR_Img_Tag
AWS_Region="ap-south-1"
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
              docker image build --no-cache -t $Local_Img_Tag --build-arg ENVN=prod .
              docker tag $Local_Img_Tag $ECR_Image_URL
              docker push $ECR_Image_URL
              aws lambda update-function-code --region $AWS_Region --function-name $Lmd_Func --image-uri $ECR_Image_URL
              echo ""
              echo ""
              echo "Lambda Function [$Lmd_Func] current state: "
              aws lambda get-function --region $AWS_Region --function-name $Lmd_Func | egrep -e "State" -e "LastModified"
              echo ""
              echo ""
              break;;
        no ) echo Exiting...;
              exit;;
        * ) echo Invalid input;;
esac