aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 891377165721.dkr.ecr.ap-south-1.amazonaws.com

docker image build --no-cache -t test-rm-allocation-process-inputs-image --build-arg ENVN=test .

docker tag test-rm-allocation-process-inputs-image 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-process-inputs:latest

docker push 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-process-inputs:latest