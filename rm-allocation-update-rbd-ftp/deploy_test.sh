docker build --no-cache -t test-rm-allocation-update-rbd-ftp-image --build-arg ENVN=test .

docker tag test-rm-allocation-update-rbd-ftp-image 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_test-rm-allocation-update-rbd-ftp:latest

docker push 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_test-rm-allocation-update-rbd-ftp:latest