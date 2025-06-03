# setting variables for the project
NAME = datascience
COMPOSE = docker-compose.yml
RED=\033[0;31m
GREEN=\033[0;32
RESET=033[0m


up: 
	docker compose -p $(NAME) -f docker-compose.yml up --build
	echo "$(GREEN) PostgreSQL DataBase and pgAdmin created $(RESET)"

# remove host entry in HOST_URL, also stop and remove containers
down:
	docker compose -p $(NAME) down
	echo "$(RED) $(NAME) DataBase was destroyed $(RESET)"

# remove all data
clean: down
	docker compose -f docker-compose.yml rm -f
	docker compose -f docker-compose.yml down --rmi all
	echo "$(RED) Directories were destroyed BOOM! $(RESET)"

# clean stopped containers
prune: clean
	docker system prune -a --volumes -f
