# Commands

## Create local image

```
docker image build --no-cache -t rm-allocation-scheduled-jobs-image .
```

## Tag docker local image

```
docker tag rm-allocation-scheduled-jobs-image 891377165721.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-scheduled-jobs:latest
```

## Push docker image

```
docker push 891377165721.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-scheduled-jobs:latest
```

# Details

## Lambda function name

```
rm-allocation-serverless-funcs_rm-allocation-scheduled-jobs
```
