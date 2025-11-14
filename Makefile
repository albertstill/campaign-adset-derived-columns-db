up:
	docker run --name aggregate-play-db -e POSTGRES_PASSWORD=password -p 5432:5432 -v ./init.sql:/docker-entrypoint-initdb.d/init.sql postgres:latest
down:
	docker stop aggregate-play-db && docker rm -v aggregate-play-db