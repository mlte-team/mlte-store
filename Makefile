# Makefile

# Run unit tests with pytest
.PHONY: test
test:
	pytest src/test

# Format source
.PHONY: format
format:
	black --line-length 80 scripts/*.py

	black --line-length 80 src/server/*.py
	black --line-length 80 src/server/backend/*.py
	black --line-length 80 src/test/*.py

# Sort imports
.PHONY: sort
sort:
	isort -e --profile black --line-length 80 scripts/*.py

	isort -e --profile black --line-length 80 src/server/*.py
	isort -e --profile black --line-length 80 src/server/backend/*.py
	isort -e --profile black --line-length 80 src/test/*.py

# Run quality assurance checks
.PHONY: qa
qa: sort format

# TMP
.PHONY: run
run:
	python src/server/main.py --backend-store-uri file:///home/turing/dev/artifact-store/storage