aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 471112573018.dkr.ecr.ap-south-1.amazonaws.com

cp -r ../rm-allocation-configurations/configsettings.py ./configsettings.py

docker image build --no-cache -t preprod-rm-pricing-fetch-inputs --build-arg ENVN=preprod  .

docker tag preprod-rm-pricing-fetch-inputs 471112573018.dkr.ecr.ap-south-1.amazonaws.com/preprod-rm-pricing-fetch-inputs:latest

docker push 471112573018.dkr.ecr.ap-south-1.amazonaws.com/preprod-rm-pricing-fetch-inputs:latest

aws lambda update-function-code --region ap-south-1 --function-name preprod-rm-pricing-fetch-inputs --image-uri 471112573018.dkr.ecr.ap-south-1.amazonaws.com/preprod-rm-pricing-fetch-inputs:latest
echo ""
echo ""
echo "Lambda Function [preprod-rm-pricing-fetch-inputs] current state: "
aws lambda get-function --region ap-south-1 --function-name preprod-rm-pricing-fetch-inputs | egrep -e "State" -e "LastModified"
echo ""
echo ""

rm -f ./configsettings.py
