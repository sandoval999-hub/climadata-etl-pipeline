.PHONY: install test lint run docker-up docker-down

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v

lint:
	black src/ tests/ main.py
	flake8 src/ tests/ main.py

run:
	python main.py --mode all

run-dry:
	python main.py --mode all --dry-run

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down
