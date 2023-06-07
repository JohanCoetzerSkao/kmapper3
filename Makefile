all:
	make -C books

count:
	find . -name "*.py" | grep -vw venv | xargs wc -l

lint:
	pylint ./skao_news/indexer.py

#	find . -name "*.py" | grep -vw venv | xargs pylint
