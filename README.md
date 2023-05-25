# kmapper3

## Abstract

Auto categorizer to mine and map words in a corpus of web pages. The steps are:

- crawls and download web pages

- read downloaded files, extract body text and split into words

- add words to database index

## Database

Table for unique words

> create table words(
> word_id int auto_increment,
> the_word varchar(255) not null,
> created_at timestamp default current_timestamp,
> primary key(word_id)
> );

Table for unique documents or pages

> create table pages(
> page_id int auto_increment,
> page_file varchar(255) not null,
> page_title varchar(512),
> page_url varchar(512),
> page_text longtext,
> created_at timestamp default current_timestamp,
> primary key(page_id)
> );

Table for relationship between words and pages

> create table pages_words(
> word_id int,
> page_id int,
> word_pos int
> );

## Running

Do a spider crawl

`scrapy crawl spider`



Download pages

`./books.sh`


`./downloader.py books.toscrape.com`


Index pages one at a time

`./books.py books.toscrape.com/catalogue/scott_pilgrims_precious_little_life_scott_pilgrim_1_987/index.html`



Index all pages

`./books.py`
