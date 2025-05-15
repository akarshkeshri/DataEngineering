IMAGE_NAME = airflow-test
TAG = latest
all:build_apiDockerFile
build:docker build --pull --rm -f Dockerfile -t$(IMAGE_NAME):$(TAG)
clean:
docker rmi
$(IMAGE_NAME):$(TAG)