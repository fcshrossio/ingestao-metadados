#!/bin/sh
rm -rf lib
rm -rf classes/rossio
rm -rf classes/org/oclc
rm -rf git

mkdir backup
cp -r data backup
cp *.sh backup

git clone https://github.com/fcshrossio/ingestao-metadados.git git
cd git
mvn -P prod assembly:assembly
cd ..

unzip -o git/target/ingestao-metadados-LinhaDeComandos.zip

cp backup/*.sh .
chmod u+x *.sh
