aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 471112573018.dkr.ecr.ap-south-1.amazonaws.com

docker build --no-cache -t rm-allocation-scheduled-jobs-image --build-arg ENVN=prod .

docker tag rm-allocation-scheduled-jobs-image 471112573018.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-scheduled-jobs:latest

docker push 471112573018.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-scheduled-jobs:latest