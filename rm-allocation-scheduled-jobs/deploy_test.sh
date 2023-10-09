docker build --no-cache -t test-rm-allocation-scheduled-jobs-image --build-arg ENVN=test .

docker tag test-rm-allocation-scheduled-jobs-image 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_test-rm-allocation-scheduled-jobs:latest

docker push 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_test-rm-allocation-scheduled-jobs:latest