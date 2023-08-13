.PHONY: lint
lint: ### Runs black, isort, flake8 and mypy for code style checks and a static analysis
	@echo "Running black"
	@python -m black --config ./pyproject.toml .

	@echo "Running isort"
	@python -m isort --sp ./pyproject.toml .

	@echo "Running flake8"
	@python -m flake8 --config ./.flake8 .

	@echo "Running mypy"
	@python -m mypy --config-file ./pyproject.toml .
