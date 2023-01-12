## iSB on AWS
* Install AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
	* Add a user using the IAM dashboard, and create an access key for that user (the users from cloudbank don't appear to have access keys?)
	* Create an access key: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey
	* Set up AWS CLI to authenticate: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html
	* Make sure your account has ECR privileges (AmazonEC2ContainerRegistryFullAccess): https://docs.aws.amazon.com/AmazonECR/latest/userguide/security-iam-awsmanpol.html
	* Get credentials for your account: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html
* Create ECR repository: https://docs.aws.amazon.com/AmazonECR/latest/userguide/getting-started-console.html
* Use AWS CLI to push to ECR: 	
	* Build docker image locally: `docker-compose --env-file .env.isamples_central -p isamples_docker_isamples_central build`
	* Get the name of the ECR repository from the ECR console.  In this example, `020235489659.dkr.ecr.us-east-1.amazonaws.com/isamples_inabox` is the name of the ECR repository and `isamples_docker_isamples_central_isamples_inabox` is the name of the image output from running the docker build command above.
	* Cut a tag: `docker tag isamples_inabox:latest 020235489659.dkr.ecr.us-east-1.amazonaws.com/isamples_inabox:latest`
	* Run the push: `docker tag isamples_docker_isamples_central_isamples_inabox:latest 020235489659.dkr.ecr.us-east-1.amazonaws.com/isamples_inabox:latest`