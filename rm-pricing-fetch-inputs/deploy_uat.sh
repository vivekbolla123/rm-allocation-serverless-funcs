aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 891377165721.dkr.ecr.ap-south-1.amazonaws.com

cp -r ../rm-allocation-configurations/configsettings.py ./configsettings.py

docker image build --no-cache -t test-rm-pricing-fetch-inputs-image --build-arg ENVN=uat  .

docker tag test-rm-pricing-fetch-inputs-image 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-pricing-fetch-inputs:latest

docker push 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-pricing-fetch-inputs:latest

aws lambda update-function-code --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-pricing-fetch-inputs --image-uri 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-pricing-fetch-inputs:latest
echo ""
echo ""
echo "Lambda Function [uat-rm-allocation-serverless-funcs-rm-pricing-fetch-inputs] current state: "
aws lambda get-function --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-pricing-fetch-inputs | egrep -e "State" -e "LastModified"
echo ""
echo ""

rm -f ./configsettings.py
