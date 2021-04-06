FROM openjdk:15-jdk-oraclelinux7

MAINTAINER Nuno Freire

ADD target/ingestao-metadados-LinhaDeComandos.zip /opt/ingestao-metadados/

WORKDIR /opt/ingestao-metadados 

RUN yum -y install unzip; \
	unzip ingestao-metadados-LinhaDeComandos.zip; \
	rm ingestao-metadados-LinhaDeComandos.zip; \
	chmod u+x oai-harvester.sh; \
	chmod u+x manager-of-harvest.sh; \
	chmod u+x indexer.sh

#CMD ["/opt/ingestao-metadados/oai-harvester.sh"]
#CMD ["/opt/ingestao-metadados/indexer.sh"]


