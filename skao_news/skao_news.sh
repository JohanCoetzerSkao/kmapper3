#!/usr/bin/bash
JFILE=skao_news.json
rm -f $JFILE
BDIR=./www.skao.int
rm -rf $BDIR
mkdir $BDIR
echo "Write $JFILE"
scrapy crawl spider -o $JFILE
# SITE="https://www.skao.int/en/news"
SITE="https://www.skao.int"

echo "Read $JFILE"
# cat $JFILE | python -m json.tool | grep -iw "URL" | cut -d: -f2- | while read BURL
cat $JFILE | jq '.[].url' | while read BURL
do
    # FURL=$(echo $BURL | tr -d ',' | tr -d '"')
    FURL=$(echo $BURL | tr -d '"')
    echo "Download $FURL"
    FPATH=$FURL     # $(echo $BURL | cut -d/ -f4- | tr '-' '_' | tr -d ',' | tr -d '"')
    FNAME=$(basename $FPATH)
    DNAME=$(dirname $FPATH)
    mkdir -p $BDIR/$DNAME
    DL_NAME="${BDIR}/${DNAME}/${FNAME}.html"
    SITE_URL="${SITE}/${FURL}"
    echo "Crawl $SITE_URL"
    echo "Write $DL_NAME"
    # echo "Run> scrapy fetch --nolog $SITE_URL"
    scrapy fetch --nolog $SITE_URL > $DL_NAME
done
