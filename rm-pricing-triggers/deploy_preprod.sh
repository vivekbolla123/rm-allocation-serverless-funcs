Lmd_Func="preprod-rm-pricing-triggers"
ECR_Domain="471112573018.dkr.ecr.ap-south-1.amazonaws.com"
Local_Img_Tag="preprod-rm-pricing-triggers"
ECR_Image_URL="$ECR_Domain/$Lmd_Func:latest"
AWS_Region="ap-south-1"

aws ecr get-login-password --region $AWS_Region | docker login --username AWS --password-stdin $ECR_Domain

cp -r ../rm-allocation-configurations/configsettings.py ./configsettings.py
cp -r ../rm-allocation-configurations/Constants.py ./Constants.py
cp -r ../rm-allocation-configurations/constants_utils.py ./constants_utils.py

docker image build --no-cache -t $Local_Img_Tag --build-arg ENVN=preprod --build-arg DD_SERVICE_NAME=$Lmd_Func .

docker tag $Local_Img_Tag $ECR_Image_URL

docker push $ECR_Image_URL

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
