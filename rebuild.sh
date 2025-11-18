#!/bin/bash
echo docker compose down -v
echo docker build -t api:v01 ./api
echo docker build -t orchestrator:v01 ./orchestrator
echo docker build -t runner:v01 ./runner
echo docker build -t inference:v01 ./inference

docker compose down -v
docker build -t api:v01 ./api
docker build -t orchestrator:v01 ./orchestrator
docker build -t runner:v01 ./runner
docker build -t inference:v01 ./inference