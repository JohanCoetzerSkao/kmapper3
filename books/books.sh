#!/usr/bin/bash
JFILE=books.toscrape.com.json
rm -f $JFILE
BDIR=./books.toscrape.com
rm -rf $BDIR
mkdir $BDIR
scrapy crawl spider -o $JFILE

cat $JFILE | python -m json.tool | grep "Book URL" | cut -d: -f2- | while read BURL
do
    FURL=$(echo $BURL | tr -d ',' | tr -d '"')
    echo "Download $FURL"
    FPATH=$(echo $BURL | cut -d/ -f4- | tr '-' '_' | tr -d ',' | tr -d '"')
    FNAME=$(basename $FPATH)
    DNAME=$(dirname $FPATH)
    mkdir -p $BDIR/$DNAME
    echo "Write $BDIR/$DNAME/$FNAME"
    scrapy fetch --nolog $FURL > $BDIR/$DNAME/$FNAME
done
