mongo-naf
=========

This is a single-class Java library to do the mapping between NAF documents and MongoDB

Kaflib library is used to manage NAF documents. By using Maven, Kaflib is automatically downloaded from Maven Central Repository at compile time.


Public interface
================

```Java

// Returns the MongoNaf instance
static MongoNaf instance(String server, int port, String dbName);


/* NAF to MongoDB */

// Send a naf document to mongoDB
void insertNafDocument(String docId, KAFDocument naf);

// Insert a specific layer of a NAF document into MongoDB, replacing the previous existing layer
void insertLayer(String docId, KAFDocument naf, String layerName);

// Insert all the LPs of a NAF document into MongoDB
void insertLinguisticProcessors(String docId, KAFDocument naf);

// Insert a specific LP into MongoDB
void insertLinguisticProcessor(String docId, LinguisticProcessor lp);

// Set the language and version information for the NAF documents created by the library
void setNafParameters(String version, String lang);


/* MongoDB to NAF */

// Create a NAF document from MongoDB
KAFDocument getNaf(String docId);

// Create a NAF document from MongoDB only containing the given layers
KAFDocument getNaf(String docId, List<String> layerNames);

// Returns wether a layername is valid or not in NAF
boolean validLayerName(String layerName);

// Gets all the LP names of a document from MongoDB
List<String> getLinguisticProcessorNames(String docId);

// Gets all the LPs of a document from MongoDB
void getLinguisticProcessors(String docId, KAFDocument naf);


// Removes a document from MongoDB
void removeDoc(String docId);
