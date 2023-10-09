docker image build --no-cache -t rm-allocation-update-rbd-image --build-arg ENVN=prod .

docker tag rm-allocation-update-rbd-image 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-update-rbd:latest

docker push 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-update-rbd:latest
