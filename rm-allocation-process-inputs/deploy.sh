docker image build --no-cache -t rm-allocation-process-inputs-image .

docker tag rm-allocation-process-inputs-image 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-process-inputs:latest

docker push 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-process-inputs:latest