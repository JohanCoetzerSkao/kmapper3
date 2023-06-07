PY_FILES = \
	./skao_news/spiders/spider.py \
	./skao_news/spiders/__init__.py \
	./skao_news/settings.py \
	./skao_news/middlewares.py \
	./skao_news/items.py \
	./skao_news/pipelines.py \
	./skao_news/__init__.py \
	./indexer.py

all:
	python -m py_compile $(PY_FILES)

lint:
	pylint $(PY_FILES)

clean:
	@find . -name "*.pyc" -print -delete
