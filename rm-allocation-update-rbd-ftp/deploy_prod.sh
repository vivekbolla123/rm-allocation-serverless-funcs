aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 471112573018.dkr.ecr.ap-south-1.amazonaws.com

docker build --no-cache -t rm-allocation-update-rbd-ftp-image --build-arg ENVN=prod .

docker tag rm-allocation-update-rbd-ftp-image 471112573018.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-update-rbd-ftp:latest

docker push 471112573018.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-update-rbd-ftp:latest