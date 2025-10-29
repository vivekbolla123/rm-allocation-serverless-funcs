aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 891377165721.dkr.ecr.ap-south-1.amazonaws.com

cp -r ../rm-allocation-configurations/configsettings.py ./configsettings.py
cp -r ../rm-allocation-configurations/Constants.py ./Constants.py
cp -r ../rm-allocation-configurations/constants_utils.py ./constants_utils.py

docker image build --no-cache -t preprod-rm-alloc-fetch-inputs --build-arg ENVN=preprod  .

docker tag preprod-rm-alloc-fetch-inputs 471112573018.dkr.ecr.ap-south-1.amazonaws.com/preprod-rm-alloc-fetch-inputs:latest

docker push 471112573018.dkr.ecr.ap-south-1.amazonaws.com/preprod-rm-alloc-fetch-inputs:latest

aws lambda update-function-code --region ap-south-1 --function-name preprod-rm-alloc-fetch-inputs --image-uri 471112573018.dkr.ecr.ap-south-1.amazonaws.com/preprod-rm-alloc-fetch-inputs:latest
echo ""
echo ""
echo "Lambda Function [preprod-rm-alloc-fetch-inputs] current state: "
aws lambda get-function --region ap-south-1 --function-name preprod-rm-alloc-fetch-inputs | egrep -e "State" -e "LastModified"
echo ""
echo ""

rm -f ./configsettings.py
rm -f ./Constants.py
rm -f ./constants_utils.py