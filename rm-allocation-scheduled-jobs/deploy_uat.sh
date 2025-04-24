aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 891377165721.dkr.ecr.ap-south-1.amazonaws.com

cp -r ../rm-allocation-configurations/configsettings.py ./configsettings.py
cp -r ../rm-allocation-configurations/Constants.py ./Constants.py
cp -r ../rm-allocation-configurations/constants_utils.py ./constants_utils.py

docker build --no-cache -t test-rm-allocation-scheduled-jobs-image --build-arg ENVN=uat .

docker tag test-rm-allocation-scheduled-jobs-image 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-scheduled-jobs:latest

docker push 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-scheduled-jobs:latest

aws lambda update-function-code --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-allocation-scheduled-jobs --image-uri 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-scheduled-jobs:latest
echo ""
echo ""
echo "Lambda Function [uat-rm-allocation-serverless-funcs-rm-allocation-scheduled-jobs] current state: "
aws lambda get-function --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-allocation-scheduled-jobs | egrep -e "State" -e "LastModified"
echo ""
echo ""

rm -f ./configsettings.py
rm -f ./Constants.py
rm -f ./constants_utils.py