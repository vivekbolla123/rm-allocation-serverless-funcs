set -e
Lmd_Func="uat-rm-allocation-serverless-funcs-rm-pricing-triggers"
ECR_Domain="891377165721.dkr.ecr.ap-south-1.amazonaws.com"
Local_Img_Tag="test-rm-pricing-triggers-image"
ECR_Image_URL="$ECR_Domain/$Lmd_Func:latest"
AWS_Region="ap-south-1"
SSM_ENV="uat"

aws ecr get-login-password --region $AWS_Region | docker login --username AWS --password-stdin $ECR_Domain

cp -r ../rm-allocation-configurations/configsettings.py ./configsettings.py
cp -r ../rm-allocation-configurations/Constants.py ./Constants.py
cp -r ../rm-allocation-configurations/constants_utils.py ./constants_utils.py

docker build --no-cache -t $Local_Img_Tag --build-arg ENVN=uat --build-arg DD_SERVICE_NAME=$Lmd_Func .

docker tag $Local_Img_Tag $ECR_Image_URL

docker push $ECR_Image_URL

aws lambda update-function-code --region $AWS_Region --function-name $Lmd_Func --image-uri $ECR_Image_URL
echo "Waiting for function update to complete..."
aws lambda wait function-updated --region $AWS_Region --function-name $Lmd_Func

# Fetch DD_API_KEY from SSM and set it on the Lambda
DD_API_KEY=$(aws ssm get-parameter --name "/rm/${SSM_ENV}/rm-allocation/DD_API_KEY" --with-decryption --query "Parameter.Value" --output text --region $AWS_Region)
EXISTING_VARS=$(aws lambda get-function-configuration --region $AWS_Region --function-name $Lmd_Func --query "Environment.Variables" --output json 2>/dev/null || echo "{}")
UPDATED_VARS=$(echo "$EXISTING_VARS" | python3 -c "import sys,json; d=json.load(sys.stdin) or {}; d['DD_API_KEY']='$DD_API_KEY'; print(json.dumps({'Variables':d}))")
aws lambda update-function-configuration --region $AWS_Region --function-name $Lmd_Func --environment "$UPDATED_VARS" > /dev/null
echo "DD_API_KEY environment variable set successfully"

echo ""
echo ""
echo "Lambda Function [$Lmd_Func] current state: "
aws lambda get-function --region $AWS_Region --function-name $Lmd_Func | egrep -e "State" -e "LastModified"
echo ""
echo ""

rm -f ./configsettings.py
rm -f ./Constants.py
rm -f ./constants_utils.py
