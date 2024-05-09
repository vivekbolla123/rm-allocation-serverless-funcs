mvn clean compile dependency:copy-dependencies -DincludeScope=runtime -U
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 891377165721.dkr.ecr.ap-south-1.amazonaws.com

export ENVN=uat

python setEnv.py
 