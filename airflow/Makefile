ROOT_DIR:=./
SRC_DIR:=./src

VENV_NAME?=venv
VENV_ACTIVATE=. $(VENV_NAME)/bin/activate
PYTHON=${VENV_NAME}/bin/python3

REQUIREMENTS_DIR:=requirements
REQUIREMENTS_LOCAL:=$(REQUIREMENTS_DIR)/requirements.txt

# .PHONY: hello venv freeze check fix clean runlocal rundocker

hello:
	@echo "hi! :)"

# HELP

help:
	@echo "make functions available, usage: $ make + (function), example: $ make venv"
	@echo "		venv: creates virtual environment (venv) and install requirements located at requirements/requirements.txt"
	@echo "		freeze: export installed libraries to requirements"
	@echo "		clean: clean the cach and files from venv"
	@echo "		isort: using the python library isort, sort the imports of dag.py"
	@echo "		autoflake_check: check unused variables and library imports by dag.py"
	@echo "		autoflake: check and rewrite the dag.py removing unused variables and library imports"
	@echo "		black: rewrite the dag.py file formatting to black format"

# VIRTUAL ENV SETUP

setup:
	sudo apt-get -y install python3.9 python3-pip 
	pip3 install virtualenv
	make venv
	. $(VENV_NAME)/bin/activate
	pip install -r $(REQUIREMENTS_LOCAL)

# DOCKER

docker:
	docker-compose up postgres
	docker-compose up initdb
	docker-compose -f docker-compose.yml up

# DEVELOPMENT

define create-venv
virtualenv venv -p python3
endef

freeze: venv
	@pip3 freeze > $(REQUIREMENTS_LOCAL)

fix: venv
	@$(AUTOPEP8) --in-place --aggressive --recursive src

clean:
	@rm -rf .cache
	@find . -type d -name __pycache__ -delete
	@rm -rf venv
	@rm -rf .tox

# PYTHON CODE FORMAT

isort:
	python3 -m isort --version
	python3 -m isort	 dags.py

autoflake_check:
	python3 -m autoflake --version
	python3 -m autoflake --remove-unused-variables --remove-all-unused-imports dags.py

autoflake:
	python3 -m autoflake --version
	python3 -m autoflake -i --remove-unused-variables --remove-all-unused-imports dags.py

black:
	python3 -m black --version
	python3 -m black .