aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 891377165721.dkr.ecr.ap-south-1.amazonaws.com

docker image build --no-cache -t test-rm-allocation-fetch-inputs-image --build-arg ENVN=uat  .

docker tag test-rm-allocation-fetch-inputs-image 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-fetch-inputs:latest

docker push 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-fetch-inputs:latest

aws lambda update-function-code --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-allocation-fetch-inputs --image-uri 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-fetch-inputs:latest
echo ""
echo ""
echo "Lambda Function [uat-rm-allocation-serverless-funcs-rm-allocation-fetch-inputs] current state: "
aws lambda get-function --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-allocation-fetch-inputs | egrep -e "State" -e "LastModified"
echo ""
echo ""