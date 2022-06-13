docker build -t project_reader -f DockerfileReader .
docker run --network project-network --rm project_reader