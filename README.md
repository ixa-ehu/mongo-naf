mongo-naf
=========

This is a single-class Java library to do the mapping between NAF documents and MongoDB

Kaflib library is used to manage NAF documents. By using Maven, Kaflib is automatically downloaded from Maven Central Repository at compile time.


Public interface
================

```Java

static MongoNaf instance(String server, int port, String dbName);


/* NAF to MongoDB */

void insertNafDocument(String docId, KAFDocument naf);

void insertNafDocument(String docId, KAFDocument naf, Integer paragraph);

void insertNafDocument(String docId, KAFDocument naf, Integer paragraph, Integer sentence);

void insertLayer(String docId, KAFDocument naf, String layerName);

void insertLayer(String docId, KAFDocument naf, String layerName, Integer paragraph);

void insertLayer(String docId, KAFDocument naf, String layerName, Integer paragraph, Integer sentence);

void insertLinguisticProcessors(String docId, KAFDocument naf);

void insertLinguisticProcessor(String docId, LinguisticProcessor lp);

void setNafParameters(String version, String lang);


/* MongoDB to NAF */

KAFDocument getNaf(String docId);

KAFDocument getNaf(String docId, List<String> layerNames);

KAFDocument getNaf(String docId, List<String> layerNames, String granularity, Integer part);

boolean validLayerName(String layerName);

List<String> getLinguisticProcessorNames(String docId);

void getLinguisticProcessors(String docId, KAFDocument naf);


void removeDoc(String docId);
