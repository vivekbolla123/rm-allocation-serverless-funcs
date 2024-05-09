mvn clean compile dependency:copy-dependencies -DincludeScope=runtime -U
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 471112573018.dkr.ecr.ap-south-1.amazonaws.com

export ENVN=prod

python setEnv.py
 