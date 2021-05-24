#!/bin/sh
rm -rf lib
rm -rf classes/rossio
rm -rf classes/org/oclc

mkdir backup
cp -r data backup
cp *.sh backup

unzip -o ingestao-metadados-LinhaDeComandos.zip

chmod u+x *.sh


