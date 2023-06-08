# Commands

## Create local image
```
docker image build --no-cache -t rm-allocation-fetch-inputs-image .
```
## Tag docker local image
```
docker tag rm-allocation-fetch-inputs-image 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-fetch-inputs:latest
```

## Push docker image
```
docker push 525872668219.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-fetch-inputs:latest
```

# Details
## Lambda function name
```
rm-allocation-serverless-funcs_rm-allocation-fetch-inputs
```
