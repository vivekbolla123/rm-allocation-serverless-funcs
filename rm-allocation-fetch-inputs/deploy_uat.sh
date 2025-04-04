aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 891377165721.dkr.ecr.ap-south-1.amazonaws.com

cp ../rm-allocation-configurations/configsettings.py ./configsettings.py
cp ../rm-allocation-configurations/Constants.py ./Constants.py
cp ../rm-allocation-configurations/constant_utils.py ./constant_utils.py

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

rm -f ./configsettings.py
rm -f ./Constants.py
rm -f ./constant_utils.py