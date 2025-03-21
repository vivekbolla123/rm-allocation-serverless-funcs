# Commands

## Create local image
```
docker image build --no-cache -t rm-allocation-update-rbd-image .
```
## Tag docker local image
```
docker tag rm-allocation-update-rbd-image 471112573018.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-update-rbd:latest
```

## Push docker image
```
docker push 471112573018.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-update-rbd:latest
```

# Details
## Lambda function name
```
rm-allocation-serverless-funcs_rm-allocation-update-rbd
```
