aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 471112573018.dkr.ecr.ap-south-1.amazonaws.com

cp -r ../rm-allocation-configurations/configsettings.py ./configsettings.py
cp -r ../rm-allocation-configurations/Constants.py ./Constants.py
cp -r ../rm-allocation-configurations/constants_utils.py ./constants_utils.py

docker image build --no-cache -t preprod-rm-alloc-process-inputs --build-arg ENVN=preprod  .

docker tag preprod-rm-alloc-process-inputs 471112573018.dkr.ecr.ap-south-1.amazonaws.com/preprod-rm-alloc-process-inputs:latest

docker push 471112573018.dkr.ecr.ap-south-1.amazonaws.com/preprod-rm-alloc-process-inputs:latest

aws lambda update-function-code --region ap-south-1 --function-name preprod-rm-alloc-process-inputs --image-uri 471112573018.dkr.ecr.ap-south-1.amazonaws.com/preprod-rm-alloc-process-inputs:latest
echo ""
echo ""
echo "Lambda Function [preprod-rm-alloc-process-inputs] current state: "
aws lambda get-function --region ap-south-1 --function-name preprod-rm-alloc-process-inputs | egrep -e "State" -e "LastModified"
echo ""
echo ""

rm -f ./configsettings.py
rm -f ./Constants.py
rm -f ./constants_utils.py