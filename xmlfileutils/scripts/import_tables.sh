#!/bin/bash

# change these according to your wiki and export date and location of the sql files
WIKI="elwikivoyage"                              # name of the wiki as it appears in downloaded files
DBNAME="elwikivoyage"                            # name of the wiki's db in your local mysql database
DATE="20170401"                                  # date as it appears in downloaded files
CMDDIR="."                                       # where the sql2txt and mwxml2sql files live
IMPORTDIR="imported"                             # directory relative to cwd, where downloaded files are located
OUTDIR="outputs"                                 # directory relative to cwd, where output files will be generated
VERSION="1.29"                                   # version of the generator in the stubs, page content files downloaded
BASEDOWNLOADURL="https://dumps.wikimedia.org"    # url to base of dumps tree for downloading

MOSTTABLES="categorylinks category change_tag externallinks geo_tags imagelinks iwlinks \
      langlinks pagelinks page_props page_restrictions protected_titles \
      redirect templatelinks"
SPECIALTABLES="page revision text"
TABLES="${MOSTTABLES} ${SPECIALTABLES}"

echo "checking if downloads are needed"
downloadsneeded=0
for table in $MOSTTABLES; do
    filename="${WIKI}-${DATE}-${table}.sql.gz"
    if [ ! -e ${IMPORTDIR}/${filename} ]; then
	downloadsneeded=1
	break
    fi
done

if [ $downloadsneeded -eq 0 ]; then
    echo "downloads not needed"
else
    echo "downloads proceeding"
    for table in $MOSTTABLES; do
        filename="${WIKI}-${DATE}-${table}.sql.gz"
        if [ ! -e ${IMPORTDIR}/${filename} ]; then
	    wget -O ${IMPORTDIR}/${filename} ${BASEDOWNLOADURL}/${WIKI}/${DATE}/$filename
        fi
    done
    echo "downloads complete"
fi

echo "checking if page, revision, text file generation needed"
generateneeded=0
for table in ${SPECIALTABLES}; do
    if [ ! -e "${OUTDIR}/${WIKI}-${DATE}-${table}.sql.gz" ]; then
	generateneeded=1
    fi
done
if [ $generateneeded -eq 0 ]; then
    echo "generation not needed"    
else
    echo "generating sql files for page, revision, text"
    ${CMDDIR}/mwxml2sql -s ${IMPORTDIR}/${WIKI}-${DATE}-stub-meta-history.xml.gz -t ${IMPORTDIR}/${WIKI}-${DATE}-pages-meta-history.xml.bz2 -f ${OUTDIR}/${WIKI}-${DATE}-history.sql.gz -m "$VERSION"
    echo "sql file generation done"
    echo "converting sql files to tab-delimited for import"
    for table in ${SPECIALTABLES}; do
        mv ${OUTDIR}/${WIKI}-${DATE}-history.sql-${table}.sql-${VERSION}.gz ${OUTDIR}/${WIKI}-${DATE}-${table}.sql.gz
    done
fi

for table in $TABLES; do
    file="${WIKI}-${DATE}-${table}.sql.gz"
    newfile=`echo $file | sed -e 's/sql.gz/tabs.gz/'`
    if [ -e "${OUTDIR}/$file" ]; then
        # if it was a converted file, use that
	infile="${OUTDIR}/$file"
    else
	# otherwise use the file we downloaded, ready for import
	infile="${IMPORTDIR}/$file"
    fi
    # convert to tab separated
    zcat $infile | ${CMDDIR}/sql2txt | gzip > ${OUTDIR}/$newfile
done
echo "tab conversion done"

echo "extracting table create statements"
for table in $MOSTTABLES; do
    python ${CMDDIR}/extract_tablecreate.py -s "${IMPORTDIR}/${WIKI}-${DATE}-${table}.sql.gz"
done
echo "table create statement extraction done"

echo "Dropping tables"
for table in $MOSTTABLES; do
    file="${WIKI}-${DATE}-${table}.sql.create"
    if [ -e ${IMPORTDIR}/${file} ]; then
        echo "DROP TABLE IF EXISTS $table ; " | mysql -u root -pnotverysecure $DBNAME
    fi
done
echo "Dropping tables done"

echo "Truncating tables"
for table in $SPECIALTABLES; do
    echo "TRUNCATE TABLE $table ; " | mysql -u root -pnotverysecure $DBNAME
done
echo "Truncating tables done"

echo "Creating tables"
for table in $MOSTTABLES; do
    file="${WIKI}-${DATE}-${table}.sql.create"
    if [ -e ${IMPORTDIR}/${file} ]; then
        cat ${IMPORTDIR}/${file} | mysql -u root -pnotverysecure $DBNAME
    fi
done
echo "Table creation done"

echo "beginning sql import"
date > import-timing.txt
CWD=`pwd`
for table in $TABLES; do
    echo "TABLE: $table"
    zcat "${CWD}/${OUTDIR}/${WIKI}-${DATE}-${table}.tabs.gz" > "${CWD}/${OUTDIR}/${WIKI}-${DATE}-${table}.tabs"
    ( \
      echo "SET autocommit=0; SET unique_checks=0; SET foreign_key_checks=0;" ;
      echo "LOAD DATA INFILE \"${CWD}/${OUTDIR}/${WIKI}-${DATE}-${table}.tabs\" INTO TABLE ${table} FIELDS OPTIONALLY ENCLOSED BY \"'\";" ;
      echo "SET autocommit=1; SET unique_checks=1; SET foreign_key_checks=1;" ; 
    ) | mysql -u root -pnotverysecure $DBNAME
    rm "${CWD}/${OUTDIR}/${WIKI}-${DATE}-${table}.tabs"
done
date >> import-timing.txt
echo "import done"
echo "ALL STEPS COMPLETE"
