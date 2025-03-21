aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 891377165721.dkr.ecr.ap-south-1.amazonaws.com

docker image build --no-cache -t test-rm-allocation-update-rbd-image --build-arg ENVN=uat .

docker tag test-rm-allocation-update-rbd-image 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd:latest

docker push 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd:latest

aws lambda update-function-code --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd --image-uri 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd:latest
echo ""
echo ""
echo "Lambda Function [uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd] current state: "
aws lambda get-function --region ap-south-1 --function-name uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd | egrep -e "State" -e "LastModified"
echo ""
echo ""