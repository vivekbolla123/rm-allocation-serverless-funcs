# Commands

## Create local image
```
docker image build --no-cache -t rm-allocation-process-inputs-image .
```
## Tag docker local image
```
docker tag rm-allocation-process-inputs-image 471112573018.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-process-inputs:latest
```

## Push docker image
```
docker push 471112573018.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-process-inputs:latest
```

# Details
## Lambda function name
```
rm-allocation-serverless-funcs_rm-allocation-process-inputs
```
