docker build -t project_writer -f DockerfileWriter .
docker run --network project-network --rm project_writer