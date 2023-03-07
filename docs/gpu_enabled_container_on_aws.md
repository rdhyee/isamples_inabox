# Configuring a GPU-enabled Docker Container on AWS
There is a significant amount of work and changes to allow for GPU-enabled ML model invocations on an AWS machine.  This doc details the steps we took to get there.

## Picking the right AWS instance type
When you create the EC2 instance, you need to select an instance type that actually has a GPU on the machine. [This doc](https://towardsdatascience.com/choosing-the-right-gpu-for-deep-learning-on-aws-d69c157d8c86) provides a good overview for the tradeoffs of the various GPU-enabled machine types.  We ultimate settled on a `g4dn.xlarge` instance type, as it offered high performance at a cost-effective price point.  We expect to spin up the instance for one-time computation, and decommission it after the inference job is complete.

## Installing GPU drivers
The first pain point in getting the instance to runnin on GPU was getting the NVIDIA drivers installed.  Various webpages seemed to indicate that it might be possible to skip this step if you selected one of the preconfigured AWS GPU-support linux images, but we just selected Ubuntu.
### NVIDIA Documentation
[The NVIDIA documentation](https://docs.nvidia.com/datacenter/tesla/tesla-installation-notes/index.html#ubuntu-lts) on driver installation was followed.  We followed the Ubuntu LTS steps:

```
sudo apt-get install linux-headers-$(uname -r)
distribution=$(. /etc/os-release;echo $ID$VERSION_ID | sed -e 's/\.//g')
wget https://developer.download.nvidia.com/compute/cuda/repos/$distribution/x86_64/cuda-keyring_1.0-1_all.deb
sudo dpkg -i cuda-keyring_1.0-1_all.deb
sudo apt-get update
sudo apt-get -y install cuda-drivers
```

Once that was all done, verification happened by running the `nvidia-smi` and observing the output:

```
Mon Mar  6 21:34:53 2023 
+---------------------------------------------------------------------------------------+
| NVIDIA-SMI 530.30.02              Driver Version: 530.30.02    CUDA Version: 12.1     |
|-----------------------------------------+----------------------+----------------------+
| GPU  Name                  Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
| Fan  Temp  Perf            Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
|                                         |                      |               MIG M. |
|=========================================+======================+======================|
|   0  Tesla T4                        Off| 00000000:00:1E.0 Off |                    0 |
| N/A   67C    P0               30W /  70W|      2MiB / 15360MiB |      4%      Default |
|                                         |                      |                  N/A |
+-----------------------------------------+----------------------+----------------------+
                                                                                         
+---------------------------------------------------------------------------------------+
| Processes:                                                                            |
|  GPU   GI   CI        PID   Type   Process name                            GPU Memory |
|        ID   ID                                                             Usage      |
|=======================================================================================|
|  No running processes found                                                           |
+---------------------------------------------------------------------------------------+
```

## Installing container toolkit
Once the driver's installed, you need to install another NVIDIA package to allow the GPU be used by Docker.
### NVIDIA Documentation
[The NVIDIA documentation](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html) on the container toolkit was followed.

```
distribution=$(. /etc/os-release;echo $ID$VERSION_ID) \
      && curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
      && curl -s -L https://nvidia.github.io/libnvidia-container/$distribution/libnvidia-container.list | \
            sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
            sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list
sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker
```
Once it was done and configured, we validated it was working by pulling the NVIDIA container image and running it:

```
sudo docker run --rm --runtime=nvidia --gpus all nvidia/cuda:11.6.2-base-ubuntu20.04 nvidia-smi

Unable to find image 'nvidia/cuda:11.6.2-base-ubuntu20.04' locally
11.6.2-base-ubuntu20.04: Pulling from nvidia/cuda
846c0b181fff: Pull complete 
b787be75b30b: Pull complete 
40a5337e592b: Pull complete 
8055c4cd4ab2: Pull complete 
a0c882e23131: Pull complete 
Digest: sha256:9928940c6e88ed3cdee08e0ea451c082a0ebf058f258f6fbc7f6c116aeb02143
Status: Downloaded newer image for nvidia/cuda:11.6.2-base-ubuntu20.04
Mon Mar  6 21:40:11 2023       
+---------------------------------------------------------------------------------------+
| NVIDIA-SMI 530.30.02              Driver Version: 530.30.02    CUDA Version: 12.1     |
|-----------------------------------------+----------------------+----------------------+
| GPU  Name                  Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
| Fan  Temp  Perf            Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
|                                         |                      |               MIG M. |
|=========================================+======================+======================|
|   0  Tesla T4                        On | 00000000:00:1E.0 Off |                    0 |
| N/A   41C    P8               13W /  70W|      2MiB / 15360MiB |      0%      Default |
|                                         |                      |                  N/A |
+-----------------------------------------+----------------------+----------------------+
                                                                                         
+---------------------------------------------------------------------------------------+
| Processes:                                                                            |
|  GPU   GI   CI        PID   Type   Process name                            GPU Memory |
|        ID   ID                                                             Usage      |
|=======================================================================================|
|  No running processes found                                                           |
+---------------------------------------------------------------------------------------+
```

## Making modifications to the Docker file(s) to allow the GPU to be used in the container
In addition to the previous steps, we needed to make a few changes to the Docker files to allow the GPU to be accessed from Docker.
### Docker Compose file
In the Docker Compose file, we need to tell Docker that we want to use the GPU.  That change looks like this:

```
diff --git a/docker-compose.yml b/docker-compose.yml
index 89f6499..8c2b99d 100644
--- a/docker-compose.yml
+++ b/docker-compose.yml
@@ -42,6 +42,7 @@ services:
         environment:
             - ISB_UVICORN_ROOT_PATH=${UVICORN_ROOT_PATH}
             - ISB_SITEMAP_PREFIX=https://${ISB_HOST}/${UVICORN_ROOT_PATH}
+            - CUDA_VISIBLE_DEVICES=0
         volumes:
             - sitemaps_data:/app/sitemaps
         secrets:
@@ -49,6 +50,13 @@ services:
             - orcid_client_secret
             - datacite_username
             - datacite_password
+        runtime: nvidia
+        deploy:
+            resources:
+                reservations:
+                    devices:
+                        - driver: nvidia
+                          capabilities: [gpu]
 
```

### ISB dockerfile
Unfortunately one of the big changes was that we need to swap out the Docker image we use to run the iSamples in a Box container.  So, instead of starting with the python image, we start with the Cuda image.  Because of this, we also need to install python separately.  Due to problems with the pip package in the `cuda` image, we instead installed the `python3.9-venv` package, which bundles python, pip, and venv all together in a virtual environment.

```
diff --git a/isb/Dockerfile b/isb/Dockerfile
index f6f5bb9..90eab16 100644
--- a/isb/Dockerfile
+++ b/isb/Dockerfile
@@ -1,6 +1,9 @@
 # syntax=docker/dockerfile:1
 # Build the main Python app
-FROM python:3.9.15 AS main
+FROM nvidia/cuda:12.0.1-runtime-ubuntu20.04 AS main

 
@@ -10,9 +13,12 @@ WORKDIR /app
 # that appear after a step that has changed.  Setting up the python environment is the most time-consuming step; put
 # it first so it only executes when it absolutely has to.
 COPY ./isamples_inabox/requirements.txt requirements.txt
-RUN pip3 install -r requirements.txt
 
-RUN apt-get update -y && apt-get install -y libgeos-dev && apt-get install -y cron
+RUN apt-get update -y && apt-get install -y libgeos-dev && apt-get install -y cron && apt-get install -y python3.9-venv && apt-get install -y git
```

### Creating the virtual environment
We opened a docker shell in the iSB container, then executed the folowing:

```
root@1a09d96d1e02:/app# python3.9 -m venv venv
root@1a09d96d1e02:/app# source venv/bin/activate
(venv) root@1a09d96d1e02:/app# pip install -r requirements.txt 
```

### Python compilation problems
While attempting to install the requirements, fasttext refused to compile.  Since we didn't need fasttext for this work, we just compiled it out of `requirements.txt` to allow everything else to work.

### Verifying it all works
After all that, we can see that we have GPU computation available in our container:

```
(venv) root@1a09d96d1e02:/app# python
Python 3.9.5 (default, Nov 23 2021, 15:27:38) 
[GCC 9.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import torch
>>> torch.cuda.is_available()
True
```