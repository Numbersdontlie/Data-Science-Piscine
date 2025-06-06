# setting variables for the project
NAME = datawarehouse
COMPOSE = docker-compose.yml
RED=\033[0;31m
GREEN=\033[0;32
RESET=033[0m


up: 
	docker compose -p $(NAME) -f ./ex00/docker-compose.yml up
	echo "$(GREEN) PostgreSQL DataBase and pgAdmin created $(RESET)"

# remove host entry in HOST_URL, also stop and remove containers
down:
	docker compose -p $(NAME) down
	echo "$(RED) $(NAME) DataBase was destroyed $(RESET)"

# rule to create the database and tables
# here i found an amazing use of redirections! thank you minishell :)
create:
	docker exec -i postgresql bash < ./ex00/load_db.sh 

# rule for customer table
customer:
	python3 ex01/customers_table.py

#rule for removing duplicates
remove:
	python3 ex02/remove_duplicates.py

# rule for file fusion
fusion:
	python3 ex03/fusion.py

# remove all data
clean: down
	docker compose -f ./ex00/docker-compose.yml rm -f
	docker compose -f ./ex00/docker-compose.yml down --rmi all
	echo "$(RED) Docker was cleaned, containers were destroyed BOOM! $(RESET)"

# clean stopped containers
prune: clean
	docker system prune -a --volumes -f