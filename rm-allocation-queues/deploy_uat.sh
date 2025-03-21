mvn clean compile dependency:copy-dependencies -DincludeScope=runtime -U
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 891377165721.dkr.ecr.ap-south-1.amazonaws.com

export ENVN=uat

python3 setEnv.py

aws lambda update-function-code --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-allocation-s3-inputs-queue --image-uri 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-s3-inputs-download:latest
echo ""
echo ""
echo "Lambda Function [uat-rm-allocation-serverless-funcs-rm-allocation-s3-inputs-queue] current state: "
aws lambda get-function --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-allocation-s3-inputs-queue | egrep -e "State" -e "LastModified"
echo ""
echo ""
 