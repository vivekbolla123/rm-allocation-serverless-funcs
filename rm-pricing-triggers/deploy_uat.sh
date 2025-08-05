aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 891377165721.dkr.ecr.ap-south-1.amazonaws.com

cp -r ../rm-allocation-configurations/configsettings.py ./configsettings.py
cp -r ../rm-allocation-configurations/Constants.py ./Constants.py
cp -r ../rm-allocation-configurations/constants_utils.py ./constants_utils.py

docker build --no-cache -t test-rm-pricing-triggers-image --build-arg ENVN=uat .

docker tag test-rm-pricing-triggers-image 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-pricing-triggers:latest

docker push 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-pricing-triggers:latest

aws lambda update-function-code --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-pricing-triggers --image-uri 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-pricing-triggers:latest
echo ""
echo ""
echo "Lambda Function [uat-rm-allocation-serverless-funcs-rm-pricing-triggers] current state: "
aws lambda get-function --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-pricing-triggers | egrep -e "State" -e "LastModified"
echo ""
echo ""

rm -f ./configsettings.py
rm -f ./Constants.py
rm -f ./constants_utils.py