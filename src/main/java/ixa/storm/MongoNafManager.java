package ixa.storm;

import ixa.kaflib.*;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;
import com.mongodb.BasicDBList;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.net.UnknownHostException;
import java.io.Serializable;


public class MongoNafManager implements Serializable {

    private static MongoNafManager instance;
    private String nafVersion;
    private String nafLang;
    private DB db;
    private DBCollection logColl;
    private DBCollection sesColl;
    private DBCollection rawColl;
    private DBCollection textColl;
    private DBCollection termsColl;
    private DBCollection entitiesColl;

    public static MongoNafManager instance(String server, int port, String dbName)
	throws MongoNafException
    {
	if (instance == null) {
	    instance = new MongoNafManager(server, port, dbName);
	}
	return instance;
    }

    private MongoNafManager(String server, int port, String dbName)
	throws MongoNafException {
	try {
	    MongoClient mongoClient = new MongoClient(server, port);
	    this.db = mongoClient.getDB(dbName);
	} catch(Exception e) {
	    throw new MongoNafException("Error connecting to MongoDB.");
	}
	this.logColl = this.db.getCollection("log");
	this.sesColl = this.db.getCollection("session");
	this.rawColl = this.db.getCollection("raw");
	this.textColl = this.db.getCollection("text");
	this.termsColl = this.db.getCollection("terms");
	this.entitiesColl = this.db.getCollection("entities");
	if (this.textColl.getIndexInfo().size() == 0) {
	    this.createIndexes();
	}
	// Default NAF values
	this.nafVersion = "mongodb_test_version";
	this.nafLang = "en";
    }

    public void defineNafParameters(String version, String lang) {
	this.nafVersion = version;
	this.nafLang = lang;
    }

    public void createIndexes() {
	//System.out.println("Creating indexes...");
	this.rawColl.createIndex(new BasicDBObject("docId", 1));
	this.textColl.createIndex(new BasicDBObject("docId", 1));
	this.termsColl.createIndex(new BasicDBObject("docId", 1));
	this.entitiesColl.createIndex(new BasicDBObject("docId", 1));
    }

    public void drop() {
	this.db.dropDatabase();
    }

    public void removeDoc(String docId) {
	this.rawColl.remove(new BasicDBObject("doc_id", docId));
	this.textColl.remove(new BasicDBObject("doc_id", docId));
	this.termsColl.remove(new BasicDBObject("doc_id", docId));
	this.entitiesColl.remove(new BasicDBObject("doc_id", docId));    
    }

    public void insertLayer(String docId, Integer sessionId, KAFDocument naf, String layerName)
    {
	if (layerName.equals("raw")) {
	    String layer = naf.getRawText();
	    this.insertRawText(layer, sessionId, docId);
	}
	else {
	    Integer firstParagraph = naf.getFirstParagraph();
	    Integer lastParagraph = firstParagraph + naf.getNumParagraphs() - 1;
	    for (int para = firstParagraph; para <= lastParagraph; para++) {
		List<Integer> sentences = naf.getSentsByParagraph(para);
		for (Integer sent : sentences) {
		    switch (layerName) {
		    case "text":
			List<WF> wfs = naf.getWFsBySent(sent);
			if (wfs.size() > 0) {
			    this.insertSentenceWFs(wfs, sessionId, docId, para, sent);
			}
			break;
		    case "terms":
			List<Term> terms = naf.getTermsBySent(sent);
			if (terms.size() > 0) {
			    this.insertSentenceTerms(terms, sessionId, docId, para, sent);
			}
			break;
		    case "entities":
			List<Entity> entities = naf.getEntitiesBySent(sent);
			if (entities.size() > 0) {
			    this.insertSentenceEntities(entities, sessionId, docId, para, sent);
			}
			break;
		    }
		}
	    }
	}
    }

    private void insertRawText(String rawText, Integer sessionId, String docId)
    {
	String id = docId + "_" + sessionId;
	DBObject doc = new BasicDBObject("_id", id)
	    .append("raw", rawText);
	try {
	    this.rawColl.save(doc);
	} catch(MongoException e) {
	}
    }

    private void insertSentenceWFs(List<WF> wfs, Integer sessionId, String docId, Integer paragraph, Integer sentence) {
	String id = docId + "_" + sessionId + "_" + paragraph + "_" + sentence;
	BasicDBObject doc = new BasicDBObject("_id", id)
	    .append("session_id", sessionId)
	    .append("doc_id", docId)
	    .append("paragraph", paragraph)
	    .append("sentence", sentence);
	List<BasicDBObject> wfObjs = new ArrayList<BasicDBObject>();
	for (WF wf : wfs) {
	    BasicDBObject wfObj = new BasicDBObject("id", wf.getId()).
		append("form", wf.getForm()).
		append("sent", wf.getSent());
	    if (wf.hasPara()) wfObj.append("para", wf.getPara());
	    if (wf.hasPage()) wfObj.append("page", wf.getPage());
	    if (wf.hasOffset()) wfObj.append("offset", wf.getOffset());
	    if (wf.hasLength()) wfObj.append("length", wf.getLength());
	    if (wf.hasXpath()) wfObj.append("xpath", wf.getXpath());
	    wfObjs.add(wfObj);
	}
	doc.append("wfs", wfObjs);
	try {
	    this.textColl.insert(doc);
	} catch(MongoException e) {
	}
    }

    public void insertSentenceTerms(List<Term> terms, Integer sessionId, String docId, Integer paragraph, Integer sentence) {
	String id = docId + "_" + sessionId + "_" + paragraph + "_" + sentence;
	BasicDBObject doc = new BasicDBObject("_id", id)
	    .append("session_id", sessionId)
	    .append("doc_id", docId)
	    .append("paragraph", paragraph)
	    .append("sentence", sentence);
	List<BasicDBObject> termObjs = new ArrayList<BasicDBObject>();
	for (Term term : terms) {
	    // Id
	    BasicDBObject termObj = new BasicDBObject("id", term.getId());
	    // Linginfo
	    if (term.hasType()) termObj.append("type", term.getType());
	    if (term.hasLemma()) termObj.append("lemma", term.getLemma());
	    if (term.hasPos()) termObj.append("pos", term.getPos());
	    if (term.hasMorphofeat()) termObj.append("morphofeat", term.getMorphofeat());
	    if (term.hasCase()) termObj.append("case", term.getCase());
	    // Anchor
	    List<String> anchor = new ArrayList<String>();
	    for (WF wf : term.getWFs()) {
		anchor.add(wf.getId());
	    }
	    termObj.append("anchor", anchor);
	    // External references
	    List<ExternalRef> externalRefs = term.getExternalRefs();
	    if (!externalRefs.isEmpty()) {
		List<DBObject> externalRefObjs = new ArrayList<DBObject>();
		for (ExternalRef extRef : externalRefs) {
		    DBObject externalRefObj = this.createExternalRefObj(extRef);
		    externalRefObjs.add(externalRefObj);
		}
		termObj.append("external_references", externalRefObjs);
	    }
	    termObjs.add(termObj);
	}
	doc.append("terms", termObjs);
	this.termsColl.save(doc);
    }

    private DBObject createExternalRefObj(ExternalRef externalRef)
    {
	BasicDBObject extRefObj = new BasicDBObject();
	extRefObj.append("resource", externalRef.getResource());
	extRefObj.append("reference", externalRef.getReference());
	if (externalRef.hasConfidence()) {
	    extRefObj.append("confidence", externalRef.getConfidence());
	}
	if (externalRef.hasExternalRef()) {
	    extRefObj.append("external_reference",
			     this.createExternalRefObj(externalRef.getExternalRef()));
	}
	return extRefObj;
    }

    public void insertSentenceEntities(List<Entity> entities, Integer sessionId, String docId, Integer paragraph, Integer sentence) {
	String id = docId + "_" + sessionId + "_" + paragraph + "_" + sentence;
	BasicDBObject doc = new BasicDBObject("_id", id)
	    .append("session_id", sessionId)
	    .append("doc_id", docId)
	    .append("paragraph", paragraph)
	    .append("sentence", sentence);
	List<BasicDBObject> entityObjs = new ArrayList<BasicDBObject>();
	for (Entity entity : entities) {
	    BasicDBObject entityObj = new BasicDBObject("id", entity.getId()).
		append("id", entity.getId());
	    if (entity.hasType()) entityObj.append("type", entity.getType());
	    if (entity.hasType()) entityObj.append("type", entity.getType());
	    List<String> anchor = new ArrayList<String>();
	    for (Term term : entity.getTerms()) {
		anchor.add(term.getId());
	    }
	    entityObj.append("anchor", anchor);
	    List<ExternalRef> externalRefs = entity.getExternalRefs();
	    if (!externalRefs.isEmpty()) {
		List<DBObject> externalRefObjs = new ArrayList<DBObject>();
		for (ExternalRef extRef : externalRefs) {
		    DBObject externalRefObj = this.createExternalRefObj(extRef);
		    externalRefObjs.add(externalRefObj);
		}
		entityObj.append("external_references", externalRefObjs);
	    }
	    entityObjs.add(entityObj);
	}
	doc.append("entities", entityObjs);
	this.entitiesColl.insert(doc);
    }


    public KAFDocument getNaf(Integer sessionId, String docId, String layerName, String granularity, Integer part)
    {
	if (!this.validLayerName(layerName)) {
	    return null;
	}

	KAFDocument naf = new KAFDocument("en", "v1");
	HashMap<String, WF> wfIndex = null;
	HashMap<String, Term> termIndex = null;
	HashMap<String, Entity> entityIndex = null;
	// Raw text
	this.queryRawTextLayer(sessionId, docId, naf);
	if (layerName.equals("raw")) return naf;
	// Text
	wfIndex = this.queryTextLayer(sessionId, docId, naf, granularity, part);
	if (layerName.equals("text")) return naf;
	// Terms    
	termIndex = this.queryTermsLayer(sessionId, docId, naf, wfIndex, granularity, part);
	if (layerName.equals("terms")) return naf;
	wfIndex = null;
	// Entities
	entityIndex = this.queryEntitiesLayer(sessionId, docId, naf, termIndex, granularity, part);
	if (layerName.equals("entities")) return naf;
	termIndex = null;

	return naf;
    }

    public KAFDocument getNaf(Integer sessionId, String docId, String layerName)
    {
	return this.getNaf(sessionId, docId, layerName, "D", null);
    }

    public KAFDocument getNaf(Integer sessionId, String docId)
    {
	return this.getNaf(sessionId, docId, "all", "D", null);
    }
    

    public boolean validLayerName(String layerName)
    {
	return layerName.equals("all")
	    || layerName.equals("raw")
	    || layerName.equals("text")
	    || layerName.equals("terms")
	    || layerName.equals("entities");
    }


    /* PRIVATE METHODS */

    private void queryRawTextLayer(Integer sessionId, String docId, KAFDocument naf)
    {
	String id = docId + "_" + sessionId;
	DBObject query = new BasicDBObject("_id", id);
	DBObject rawTextObj = this.rawColl.findOne(query);
	String raw = (String) rawTextObj.get("raw");
	naf.setRawText(raw);
    }

    private HashMap<String, WF> queryTextLayer(Integer sessionId, String docId, KAFDocument naf, String granularity, Integer part)
    {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBCursor cursor = this.textColl.find(query);
	HashMap<String, WF> wfIndex = new HashMap<String, WF>();
	while (cursor.hasNext()) {
	    List<DBObject> mongoWfs = (List<DBObject>) cursor.next().get("wfs");
	    for (DBObject mongoWf : mongoWfs) {
		WF wf = naf.newWF((String) mongoWf.get("id"), (String) mongoWf.get("form"), (Integer) mongoWf.get("sent"));
		if (mongoWf.containsField("para")) wf.setPara((Integer) mongoWf.get("para"));
		if (mongoWf.containsField("page")) wf.setPage((Integer) mongoWf.get("page"));
		if (mongoWf.containsField("offset")) wf.setOffset((Integer) mongoWf.get("offset"));
		if (mongoWf.containsField("length")) wf.setLength((Integer) mongoWf.get("length"));
		if (mongoWf.containsField("xpath")) wf.setXpath((String) mongoWf.get("xpath"));
		wfIndex.put(wf.getId(), wf);
	    }
	}
	return wfIndex;
    }

    private HashMap<String, Term> queryTermsLayer(Integer sessionId, String docId, KAFDocument naf, HashMap<String, WF> wfIndex, String granularity, Integer part) {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBCursor cursor = this.termsColl.find(query);
	HashMap<String, Term> termIndex = new HashMap<String, Term>();
	while (cursor.hasNext()) {
	    List<DBObject> mongoTerms = (List<DBObject>) cursor.next().get("terms");
	    for (DBObject mongoTerm : mongoTerms) {
		BasicDBList wfIds = (BasicDBList) mongoTerm.get("anchor");
		Span<WF> wfs = KAFDocument.newWFSpan();
		for (int i = 0; i < wfIds.size(); i++) {
		    wfs.addTarget(wfIndex.get((String) wfIds.get(i)));
		}
		Term term = naf.newTerm((String) mongoTerm.get("id"), wfs);
		if (mongoTerm.containsField("type")) term.setType((String) mongoTerm.get("type"));
		if (mongoTerm.containsField("lemma")) term.setLemma((String) mongoTerm.get("lemma"));
		if (mongoTerm.containsField("pos")) term.setPos((String) mongoTerm.get("pos"));
		if (mongoTerm.containsField("morphofeat")) term.setMorphofeat((String) mongoTerm.get("morphofeat"));
		if (mongoTerm.containsField("case")) term.setCase((String) mongoTerm.get("case"));
		termIndex.put(term.getId(), term);
	    }
	}
	return termIndex;
    }
    
    private HashMap<String, Entity> queryEntitiesLayer(Integer sessionId, String docId, KAFDocument naf, HashMap<String, Term> termIndex, String granularity, Integer part) {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBCursor cursor = this.entitiesColl.find(query);
	HashMap<String, Entity> entityIndex = new HashMap<String, Entity>();
	while (cursor.hasNext()) {
	    List<DBObject> mongoEntities = (List<DBObject>) cursor.next().get("entities");
	    for (DBObject mongoEntity : mongoEntities) {
		BasicDBList termIds = (BasicDBList) mongoEntity.get("anchor");
		Span<Term> terms = KAFDocument.newTermSpan();
		for (int i = 0; i < termIds.size(); i++) {
		    String tid = (String) termIds.get(i);
		    Term term = termIndex.get(tid);		   
		    terms.addTarget(term);
		}
		List<Span<Term>> termSpans = new ArrayList<Span<Term>>();
		termSpans.add(terms);
		Entity entity = naf.newEntity((String) mongoEntity.get("id"), termSpans);
		if (mongoEntity.containsField("type")) entity.setType((String) mongoEntity.get("type"));
		entityIndex.put(entity.getId(), entity);
	    }
	}
	return entityIndex;
    }

    private BasicDBObject createQuery(Integer sessionId, String docId, String granularity, Integer part)
    {
	BasicDBObject query = new BasicDBObject("session_id", sessionId).
	    append("doc_id", docId);
	if (granularity == "P") {
	    query.append("paragraph", part);
	} else if (granularity == "S") {
	    query.append("sentence", part);
	}
	return query;
    }





    /*
    public int getNextSessionId()
    {
	DBObject ref = new BasicDBObject();
	DBObject keys = new BasicDBObject("session_id", 1).append("_id", 0);
	DBObject orderBy = new BasicDBObject("session_id", -1);
	DBCursor sessions = this.sesColl.find(ref, keys);
	if (sessions.count() == 0) {
	    return 1;
	}
	Integer lastId = (Integer)sessions.sort(orderBy).limit(1).next().get("session_id");
	return lastId + 1;
    }

    public boolean sessionIdExists(Integer sessionId)
    {
	DBObject query = new BasicDBObject("session_id", sessionId);
	DBCursor session = this.sesColl.find(query);
	return session.count() > 0;
    }

    public void newSession(Integer sessionId, Integer numDoc)
    {
	DBObject query = new BasicDBObject("session_id", sessionId)
	    .append("num_docs", numDoc);
        this.sesColl.insert(query);
    }

    public void updateSessionDocuments(Integer sessionId, Integer numDoc)
    {
	DBObject ref = new BasicDBObject("session_id", sessionId);
	DBObject query = new BasicDBObject("$inc", new BasicDBObject("num_docs", numDoc));
        this.sesColl.update(ref, query);
    }

    public void writeLogEntry(Integer sessionId, String entry)
    {
	BasicDBObject entryDoc = new BasicDBObject("session_id", sessionId)
	    .append("value", entry);
	try {
	    this.logColl.insert(entryDoc);
	} catch(MongoException e) {}
    }

    public List<String> getLog(Integer sessionId)
    {
	BasicDBObject query = new BasicDBObject("session_id", sessionId);
	DBCursor logEntries = this.logColl.find(query);
	List<String> log = new ArrayList<String>();
	while (logEntries.hasNext()) {
	    String entry = (String) logEntries.next().get("value");
	    log.add(entry);
	}
	return log;
    }
    */

}
