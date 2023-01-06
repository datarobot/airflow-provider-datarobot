SHELL := bash

.PHONY: lint unit-tests typecheck black

lint:
	flake8

black:
	black .

unit-tests:
	pytest -vv tests/unit/

typecheck:
	find . -type f -name "*.py" | grep -v ".git" | xargs mypy --config-file .mypy.ini

# Copyright Notices are handled by the next two targets
# See .licenserc.yaml for configuration
.PHONY: fix-licenses
fix-licenses:
	docker run  --rm -v $(CURDIR):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 -v info -c .licenserc.yaml header fix

.PHONY: check-licenses
check-licenses:
	docker run  --rm -v $(CURDIR):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 -v info -c .licenserc.yaml header check
