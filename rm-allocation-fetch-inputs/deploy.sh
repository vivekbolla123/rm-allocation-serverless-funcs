docker image build --no-cache -t rm-allocation-fetch-inputs-image .

docker tag rm-allocation-fetch-inputs-image 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-fetch-inputs:latest

docker push 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-fetch-inputs:latest