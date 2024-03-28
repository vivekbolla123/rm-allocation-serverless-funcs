aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 891377165721.dkr.ecr.ap-south-1.amazonaws.com

docker build --no-cache -t test-rm-allocation-update-rbd-ftp-image --build-arg ENVN=uat .

docker tag test-rm-allocation-update-rbd-ftp-image 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd-ftp:latest

docker push 891377165721.dkr.ecr.ap-south-1.amazonaws.com/uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd-ftp:latest