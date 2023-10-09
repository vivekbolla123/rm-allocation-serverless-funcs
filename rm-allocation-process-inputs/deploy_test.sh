docker image build --no-cache -t test-rm-allocation-process-inputs-image --build-arg ENVN=test .

docker tag test-rm-allocation-process-inputs-image 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_test-rm-allocation-process-inputs:latest

docker push 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_test-rm-allocation-process-inputs:latest