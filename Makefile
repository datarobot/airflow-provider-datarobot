SHELL := bash

# Determine the operating system
# "darwin" or "linux"
OS_NAME := $(shell uname -s | tr A-Z a-z)
ifeq ($(OS_NAME), darwin)
	ASTRO_CMD=brew install astro
else
	ASTRO_CMD=curl -sSL install.astronomer.io | sudo bash -s
endif

.PHONY: help
help:
	@echo "DataRobot Airflow Provider"
	@echo "=========================="
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9%-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo

##@ Linting

.PHONY: lint
lint:  ## Lint using ruff
	ruff check .

.PHONY: lint-fix
lint-fix:  ## Fix using ruff
	ruff check . --fix

.PHONY: format
format:  ## Format using ruff
	ruff format .

.PHONY: format-no-fix
format-no-fix:  ## Check formatting using ruff
	ruff format . --check

.PHONY: typecheck
typecheck:  ## Type check using mypy
	mypy --config-file pyproject.toml datarobot_provider

##@ Development

.PHONY: clean
clean:  ## Clean up misc files like pytest and mypy cache for example
	rm -f dist/*
	rm -rf .mypy_cache
	find . -type f \( -name '*.py[co]' -o -name '*~' -o -name '@*' -o -name '#*#' -o -name '*.orig' -o -name '*.rej' -o -name '.chunk_*' -o -name 'tmp*' \) -delete
	find . -type d -name __pycache__ -delete
	rm -rf build/* && rm -rf airflow_provider_datarobot.egg-info
	rm -rf build/* && rm -rf airflow_provider_datarobot_early_access.egg-info

.PHONY: req
req: clean  ## Install requirements
	pip install --upgrade setuptools
	pip install 'pip<24.1'
	pip install -e .

.PHONY: req-dev
req-dev: clean  ## Install dev. requirements
	pip install --upgrade setuptools
	pip install 'pip<24.1'
	pip install -e ".[dev]"

.PHONY: req-dev-docs
req-dev-docs: clean  ## Install dev. and docs. requirements
	pip install --upgrade setuptools
	pip install 'pip<24.1'
	pip install -e ".[dev,docs]"

.PHONY: test-harness
test-harness:
	pytest -vv tests/unit/ --junit-xml=unit_test_report.xml

.PHONY: unit-tests
unit-tests:
	pytest -vv tests/unit/

.PHONY: html-docs
html-docs:
	cd docs && $(MAKE) html

.PHONY: test-docs
test-docs:
	cd docs && $(MAKE) doctest

.PHONY: test-docs-harness
test-docs-harness:
	$(MAKE) test-docs

# Copyright Notices are handled by the next two targets
# See .licenserc.yaml for configuration
.PHONY: fix-licenses
fix-licenses:
	docker run  --rm -v $(CURDIR):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 -v info -c .licenserc.yaml header fix

.PHONY: check-licenses
check-licenses:
	docker run  --rm -v $(CURDIR):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 -v info -c .licenserc.yaml header check

.PHONY: install-astro
install-astro:
	$(ASTRO_CMD)

.PHONY: create-astro-dev
create-astro-dev:
	mkdir astro-dev
	cd astro-dev && astro dev init
	grep -qF -- 'RUN pip install -r \"/usr/local/airflow/requirements_dev.txt\"' ./astro-dev/Dockerfile || \
	echo "RUN pip install -r \"/usr/local/airflow/requirements_dev.txt\"" >> ./astro-dev/Dockerfile
	grep -qF -- 'AIRFLOW__CORE__TEST_CONNECTION=Enabled' ./astro-dev/.env || \
	echo "AIRFLOW__CORE__TEST_CONNECTION=Enabled" >> ./astro-dev/.env

.PHONY: clean-astro-dev
clean-astro-dev:  ## Completely wipeout an existing development environment and reset it
	-$(MAKE) kill-astro-dev
	rm -rf astro-dev
	$(MAKE) create-astro-dev

.PHONY: start-astro-dev
start-astro-dev:
	cd astro-dev && astro dev start --no-cache

.PHONY: stop-astro-dev
stop-astro-dev:
	cd astro-dev && astro dev stop

.PHONY: kill-astro-dev
kill-astro-dev:
	cd astro-dev && astro dev kill

.PHONY: build-astro-dev
build-astro-dev:
	-$(MAKE) stop-astro-dev
	rm -rf ./dist
	$(MAKE) build-release
	cp -p "`ls -dtr1 ./dist/*.whl | sort -n | tail -1`" "./astro-dev/"
	echo "/usr/local/airflow/`find ./dist/*.whl -exec basename {} \; | sort -n | tail -1`" > \
		./astro-dev/requirements_dev.txt
	$(MAKE) copy-examples-astro-dev
	$(MAKE) start-astro-dev

.PHONY: build-astro-dev-early-access
build-astro-dev-early-access:
	-$(MAKE) stop-astro-dev
	rm -rf ./dist
	$(MAKE) build-early-access
	cp -p "`ls -dtr1 ./dist/*.whl | sort -n | tail -1`" "./astro-dev/"
	echo "/usr/local/airflow/`find ./dist/*.whl -exec basename {} \; | sort -n | tail -1`" > \
		./astro-dev/requirements_dev.txt
	$(MAKE) copy-examples-astro-dev
	$(MAKE) copy-experimental-examples-astro-dev
	$(MAKE) start-astro-dev

.PHONY: copy-examples-astro-dev
copy-examples-astro-dev:
	cp -r ./datarobot_provider/example_dags/* ./astro-dev/dags/

.PHONY: copy-experimental-examples-astro-dev
copy-experimental-examples-astro-dev:
	cp -r ./datarobot_provider/_experimental/example_dags/* ./astro-dev/dags/

.PHONY: autogen-operators
autogen-operators:
	python ./datarobot_provider/autogen/generate_operator.py --whitelist ./datarobot_provider/autogen/whitelist.yaml --output_folder ./datarobot_provider/operators/gen

.PHONY: test-development-container
test-development-container:
	$(MAKE) install-astro
	$(MAKE) clean-astro-dev
	$(MAKE) build-astro-dev
	chmod +x ./scripts/test_development_container.sh
	./scripts/test_development_container.sh

##@ Building

.PHONY: build-release
build-release: clean  ## Make release build
	pip3 install --no-cache-dir --upgrade pip setuptools wheel twine
	python setup.py sdist bdist_wheel

.PHONY: build-early-access
build-early-access: clean  ## Make early access build
	pip3 install --no-cache-dir --upgrade pip setuptools wheel twine
	python setup_early_access.py sdist bdist_wheel
