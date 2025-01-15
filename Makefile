SHELL := bash

# Determine the operating system
# "darwin" or "linux"
OS_NAME := $(shell uname -s | tr A-Z a-z)
ifeq ($(OS_NAME), darwin)
	ASTRO_CMD=brew install astro
else
	ASTRO_CMD=curl -sSL install.astronomer.io | sudo bash -s
endif

.PHONY: black isort lint typecheck check-licenses fix-licenses unit-tests

req:
	pip install -r requirements.txt

lint:
	flake8

black:
	black .

isort:
	isort .

unit-tests:
	pytest -vv tests/unit/

typecheck:
	find . -type f -name "*.py" | grep -v ".git" | xargs mypy --config-file .mypy.ini

# Copyright Notices are handled by the next two targets
# See .licenserc.yaml for configuration
fix-licenses:
	docker run  --rm -v $(CURDIR):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 -v info -c .licenserc.yaml header fix

check-licenses:
	docker run  --rm -v $(CURDIR):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 -v info -c .licenserc.yaml header check

install-astro:
	$(ASTRO_CMD)

create-astro-dev:
	mkdir astro-dev
	cd astro-dev && astro dev init
	echo "RUN pip install -r \"/usr/local/airflow/requirements_dev.txt\"" >> ./astro-dev/Dockerfile

reset-astro-dev:
	rm -rf astro-dev
	$(MAKE) create-astro-dev

start-astro-dev:
	cd astro-dev && astro dev start

stop-astro-dev:
	cd astro-dev && astro dev stop

build-astro-dev:
	$(MAKE) stop-astro-dev
	cd astro-dev && astro dev stop
	rm -rf ./dist
	pip install --upgrade build
	python -m build
	cp -r ./datarobot_provider/example_dags/* ./astro-dev/dags/
	cp -p "`ls -dtr1 ./dist/*.whl | sort -n | tail -1`" "./astro-dev/"
	echo "/usr/local/airflow/`find ./dist/*.whl -exec basename {} \; | sort -n | tail -1`" > \
 		./astro-dev/requirements_dev.txt
	$(MAKE) start-astro-dev
