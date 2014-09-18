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
import java.util.Map;
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
    private DBCollection depsColl;
    private DBCollection constituentsColl;
    private DBCollection chunksColl;
    private DBCollection corefsColl;
    private DBCollection opinionsColl;
    private DBCollection srlColl;

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
	this.depsColl = this.db.getCollection("deps");
	this.constituentsColl = this.db.getCollection("constituents");
	this.chunksColl = this.db.getCollection("chunks");
	this.corefsColl = this.db.getCollection("coreferences");
	this.opinionsColl = this.db.getCollection("opinions");
	this.srlColl = this.db.getCollection("srl");
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
	String idField = "doc_id";
	this.rawColl.createIndex(new BasicDBObject(idField, 1));
	this.textColl.createIndex(new BasicDBObject(idField, 1));
	this.termsColl.createIndex(new BasicDBObject(idField, 1));
	this.entitiesColl.createIndex(new BasicDBObject(idField, 1));
	this.depsColl.createIndex(new BasicDBObject(idField, 1));
	this.constituentsColl.createIndex(new BasicDBObject(idField, 1));
	this.chunksColl.createIndex(new BasicDBObject(idField, 1));
	this.corefsColl.createIndex(new BasicDBObject(idField, 1));
	this.opinionsColl.createIndex(new BasicDBObject(idField, 1));
	this.srlColl.createIndex(new BasicDBObject(idField, 1));
    }

    public void drop() {
	this.db.dropDatabase();
    }

    public void removeDoc(String docId) {
	String idField = "doc_id";
	this.rawColl.remove(new BasicDBObject(idField, docId));
	this.textColl.remove(new BasicDBObject(idField, docId));
	this.termsColl.remove(new BasicDBObject(idField, docId));
	this.entitiesColl.remove(new BasicDBObject(idField, docId));  
	this.depsColl.remove(new BasicDBObject(idField, docId));    
	this.constituentsColl.remove(new BasicDBObject(idField, docId));   
	this.chunksColl.remove(new BasicDBObject(idField, docId));   
	this.corefsColl.remove(new BasicDBObject(idField, docId));   
	this.opinionsColl.remove(new BasicDBObject(idField, docId));   
	this.srlColl.remove(new BasicDBObject(idField, docId)); 
    }

    public void insertNafDocument(String docId, Integer sessionId, KAFDocument naf)
    {
	this.insertNafDocument(docId, sessionId, naf, null, null);
    }
	
    public void insertNafDocument(String docId, Integer sessionId, KAFDocument naf, Integer paragraph)
    {
	this.insertNafDocument(docId, sessionId, naf, paragraph, null);
    }

    public void insertNafDocument(String docId, Integer sessionId, KAFDocument naf, Integer paragraph, Integer sentence)
    {
	this.insertLayer(docId, sessionId, naf, "raw", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "text", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "terms", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "entities", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "deps", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "constituents", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "chunks", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "coreferences", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "opinions", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "srl", paragraph, sentence);
    }

    public void insertLayer(String docId, Integer sessionId, KAFDocument naf, String layerName)
    {
	this.insertLayer(docId, sessionId, naf, layerName, null, null);
    }

    public void insertLayer(String docId, Integer sessionId, KAFDocument naf, String layerName, Integer paragraph)
    {
	this.insertLayer(docId, sessionId, naf, layerName, paragraph, null);
    }

    public void insertLayer(String docId, Integer sessionId, KAFDocument naf, String layerName, Integer paragraph, Integer sentence)
    {
	if (layerName.equals("raw")) {
	    String layer = naf.getRawText();
	    this.insertRawText(layer, sessionId, docId);
	}
	else {
	    List<DBObject> annDBObjs = new ArrayList<DBObject>();
	    DBCollection docCollection = null;
	    switch (layerName) {
	    case "text":
		for (WF annotation : naf.getWFs()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.textColl;
		break;
	    case "terms":
	        for (Term annotation : naf.getTerms()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.termsColl;
		break;
	    case "entities":
		for (Entity annotation : naf.getEntities()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.entitiesColl;
		break;
	    case "deps":
	        for (Dep annotation : naf.getDeps()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.depsColl;
		break;
	    case "constituents":
	        for (Tree annotation : naf.getConstituents()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.constituentsColl;
		break;
	    case "chunks":
	        for (Chunk annotation : naf.getChunks()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.chunksColl;
		break;
	    case "coreferences":
	        for (Coref annotation : naf.getCorefs()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.corefsColl;
		break;
	    case "opinions":
	        for (Opinion annotation : naf.getOpinions()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.opinionsColl;
		  break;
	    case "srl":
	        for (Predicate annotation : naf.getPredicates()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.srlColl;
		break;
	    }
	    if (annDBObjs.size() > 0) {
		this.insertDocument(annDBObjs, docCollection, sessionId, docId, paragraph, sentence);
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

    private void insertDocument(List<DBObject> annotations, DBCollection collection, Integer sessionId, String docId, Integer paragraph, Integer sentence) {
	BasicDBObject doc = new BasicDBObject()
	    .append("session_id", sessionId)
	    .append("doc_id", docId);
	String id = sessionId + "_" + docId;
	if (paragraph != null) {
	    id += "_" + paragraph;
	    doc.append("paragraph", paragraph);
	    if (sentence != null) {
		id += "_" + sentence;
		doc.append("sentence", sentence);
	    }
	}
	doc.append("_id", id);
	doc.append("annotations", annotations);
	try {
	    collection.insert(doc);
	} catch(MongoException e) {
	}
    }

    private DBObject map(WF wf) {
	BasicDBObject wfObj = new BasicDBObject("id", wf.getId()).
	    append("form", wf.getForm()).
	    append("sent", wf.getSent());
	if (wf.hasPara()) wfObj.append("para", wf.getPara());
	if (wf.hasPage()) wfObj.append("page", wf.getPage());
	if (wf.hasOffset()) wfObj.append("offset", wf.getOffset());
	if (wf.hasLength()) wfObj.append("length", wf.getLength());
	if (wf.hasXpath()) wfObj.append("xpath", wf.getXpath());
	return wfObj;
    }

    private DBObject map(Term term) {
	// Id
	BasicDBObject termObj = new BasicDBObject("id", term.getId());
	// Linginfo
	if (term.hasType()) termObj.append("type", term.getType());
	if (term.hasLemma()) termObj.append("lemma", term.getLemma());
	if (term.hasPos()) termObj.append("pos", term.getPos());
	if (term.hasMorphofeat()) termObj.append("morphofeat", term.getMorphofeat());
	if (term.hasCase()) termObj.append("case", term.getCase());
	if (term.hasSentiment()) { // Sentiment
	    Term.Sentiment sentiment = term.getSentiment();
	    BasicDBObject sentimentObj = new BasicDBObject();
	    if (sentiment.hasResource()) sentimentObj.append("resource", sentiment.getResource());
	    if (sentiment.hasPolarity()) sentimentObj.append("polarity", sentiment.getPolarity());
	    if (sentiment.hasStrength()) sentimentObj.append("strength", sentiment.getStrength());
	    if (sentiment.hasSubjectivity()) sentimentObj.append("subjectivity", sentiment.getSubjectivity());
	    if (sentiment.hasSentimentSemanticType()) sentimentObj.append("sentimentSemanticType", sentiment.getSentimentSemanticType());
	    if (sentiment.hasSentimentModifier()) sentimentObj.append("sentimentModifier", sentiment.getSentimentModifier());
	    if (sentiment.hasSentimentMarker()) sentimentObj.append("sentimentMarker", sentiment.getSentimentMarker());
	    if (sentiment.hasSentimentProductFeature()) sentimentObj.append("sentimentProductFeature", sentiment.getSentimentProductFeature());
	    termObj.append("sentiment", sentimentObj);
	}
	String headId = (term.getHead() != null) ? term.getHead().getId() : "";
	List<Term> components = term.getComponents();
	if (components.size() > 0) {
	    List<DBObject> componentsObj = new ArrayList<DBObject>();
	    for (Term component : components) {
		BasicDBObject componentObj = new BasicDBObject("id", component.getId());
		if (component.hasType()) componentObj.append("type", component.getType());
		if (component.hasLemma()) componentObj.append("lemma", component.getLemma());
		if (component.hasPos()) componentObj.append("pos", component.getPos());
		if (component.hasMorphofeat()) componentObj.append("morphofeat", component.getMorphofeat());
		if (component.hasCase()) componentObj.append("case", component.getCase());
		if (component.getId() == headId) componentObj.append("head", true);
		List<ExternalRef> externalRefs = component.getExternalRefs();
		if (!externalRefs.isEmpty()) {
		    List<DBObject> externalRefObjs = new ArrayList<DBObject>();
		    for (ExternalRef extRef : externalRefs) {
			DBObject externalRefObj = this.map(extRef);
			externalRefObjs.add(externalRefObj);
		    }
		    componentObj.append("external_references", externalRefObjs);
		}
		componentsObj.add(componentObj);
	    }
	    termObj.append("components", componentsObj);
	}
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
		DBObject externalRefObj = this.map(extRef);
		externalRefObjs.add(externalRefObj);
	    }
	    termObj.append("external_references", externalRefObjs);
	}
	return termObj;
    }

    private DBObject map(Entity entity) {
	BasicDBObject entityObj = new BasicDBObject("id", entity.getId());
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
		DBObject externalRefObj = this.map(extRef);
		externalRefObjs.add(externalRefObj);
	    }
	    entityObj.append("external_references", externalRefObjs);
	}
	return entityObj;
    }

    private DBObject map(Dep dep) {
	// Linginfo
	BasicDBObject depObj = new BasicDBObject("rfunc", dep.getRfunc());
	if (dep.hasCase()) depObj.append("case", dep.getCase());
	// Anchor (???)
	depObj.append("from", dep.getFrom().getId()).
	    append("to", dep.getTo().getId());
	return depObj;
    }

    private DBObject map(Tree tree) {
	BasicDBObject treeObj = new BasicDBObject();
	treeObj.append("terminals", new ArrayList<BasicDBObject>());
	treeObj.append("non_terminals", new ArrayList<BasicDBObject>());
	treeObj.append("edges", new ArrayList<BasicDBObject>());
	TreeNode root = tree.getRoot();
	this.map(root, null, treeObj);
	return treeObj;
    }

    private void map(TreeNode node, TreeNode parentNode, BasicDBObject treeObj) {
	if (parentNode != null) {
	    String parentId = parentNode.getId();
	    BasicDBObject edgeObj = new BasicDBObject("id", node.getEdgeId());
	    edgeObj.append("from", node.getId());
	    edgeObj.append("to", parentId);
	    if (node.getHead())
		edgeObj.append("head", "yes");
	    ((List<DBObject>) treeObj.get("edges")).add(edgeObj);
	}
	BasicDBObject nodeObj = new BasicDBObject("id", node.getId());
	if (node.isTerminal()) {
	    List<String> anchor = new ArrayList<String>();
	    for (Term term : ((Terminal) node).getSpan().getTargets()) {
		anchor.add(term.getId());
	    }
	    nodeObj.append("anchor", anchor);
	    ((List<DBObject>) treeObj.get("terminals")).add(nodeObj);
	} else {
	    nodeObj.append("label", ((NonTerminal) node).getLabel());
	    ((List<DBObject>) treeObj.get("non_terminals")).add(nodeObj);
	    for (TreeNode child : ((NonTerminal) node).getChildren()) {
		this.map(child, node, treeObj);
	    }
	}
    }

    private DBObject map(Chunk chunk) {
	// Linginfo
	BasicDBObject chunkObj = new BasicDBObject("id", chunk.getId());
	if (chunk.hasPhrase()) chunkObj.append("phrase", chunk.getPhrase());
	if (chunk.hasCase()) chunkObj.append("case", chunk.getCase());
	// Anchor
	List<String> anchor = new ArrayList<String>();
	for (Term term : chunk.getSpan().getTargets()) {
	    anchor.add(term.getId());
	}
	chunkObj.append("anchor", anchor);
	return chunkObj;
    }

    private DBObject map(Coref coref) {
	// Linginfo
	BasicDBObject corefObj = new BasicDBObject("id", coref.getId());
	// Anchor
	List<List<String>> anchor = new ArrayList<List<String>>();
	for (Span<Term> span : coref.getSpans()) {
	    List<String> spanTerms = new ArrayList<String>();
	    for (Term term : span.getTargets()) {
		spanTerms.add(term.getId());
	    }
	    anchor.add(spanTerms);
	}
	corefObj.append("anchor", anchor);
	return corefObj;
    }
    
    private DBObject map(Opinion opinion) {
	// Id
	BasicDBObject opinionObj = new BasicDBObject("id", opinion.getId());
        // OpinionHolder
	Opinion.OpinionHolder opHolder = opinion.getOpinionHolder();
	BasicDBObject opHolderObj = new BasicDBObject();
	if (opHolder.hasType()) opHolderObj.append("type", opHolder.getType());
	List<String> anchor = new ArrayList<String>();
	for (Term term : opHolder.getSpan().getTargets()) {
	    anchor.add(term.getId());
	}
	opHolderObj.append("anchor", anchor);
	opinionObj.append("opinion_holder", opHolderObj);
	// OpinionTarget
	Opinion.OpinionTarget opTarget = opinion.getOpinionTarget();
	BasicDBObject opTargetObj = new BasicDBObject();
	anchor = new ArrayList<String>();
	for (Term term : opTarget.getSpan().getTargets()) {
	    anchor.add(term.getId());
	}
	opTargetObj.append("anchor", anchor);
	opinionObj.append("opinion_target", opTargetObj);
	// OpinionExpression
	Opinion.OpinionExpression opExpression = opinion.getOpinionExpression();
	BasicDBObject opExpressionObj = new BasicDBObject();
	if (opExpression.hasPolarity()) opExpressionObj.append("polarity", opExpression.getPolarity());
	if (opExpression.hasStrength()) opExpressionObj.append("strength", opExpression.getStrength());
	if (opExpression.hasSubjectivity()) opExpressionObj.append("subjectivity", opExpression.getSubjectivity());
	if (opExpression.hasSentimentSemanticType()) opExpressionObj.append("sentiment_semantic_type", opExpression.getSentimentSemanticType());
	if (opExpression.hasSentimentProductFeature()) opExpressionObj.append("sentiment_product_feature", opExpression.getSentimentProductFeature());
	anchor = new ArrayList<String>();
	for (Term term : opExpression.getSpan().getTargets()) {
	    anchor.add(term.getId());
	}
	opExpressionObj.append("anchor", anchor);
	opinionObj.append("opinion_expression", opExpressionObj);

	return opinionObj;
    }

    private DBObject map(Predicate predicate) {
	// Id
	BasicDBObject predicateObj = new BasicDBObject("id", predicate.getId());
	// Linginfo
	if (predicate.hasUri()) predicateObj.append("uri", predicate.getUri());
	if (predicate.hasConfidence()) predicateObj.append("confidence", (Float) predicate.getConfidence());
	// Anchor
	List<String> anchor = new ArrayList<String>();
	for (Term term : predicate.getSpan().getTargets()) {
	    anchor.add(term.getId());
	}
	predicateObj.append("anchor", anchor);
	// External references
	List<ExternalRef> externalRefs = predicate.getExternalRefs();
	if (!externalRefs.isEmpty()) {
	    List<DBObject> externalRefObjs = new ArrayList<DBObject>();
	    for (ExternalRef extRef : externalRefs) {
		DBObject externalRefObj = this.map(extRef);
		externalRefObjs.add(externalRefObj);
	    }
	    predicateObj.append("external_references", externalRefObjs);
	}
	// Roles
	List<Predicate.Role> roles = predicate.getRoles();
	List<DBObject> roleObjs = new ArrayList<DBObject>();
	for (Predicate.Role role : roles) {
	    BasicDBObject roleObj = new BasicDBObject("id", role.getId()).
		append("sem_role", role.getSemRole());
	    // Anchor
	    anchor = new ArrayList<String>();
	    for (Term term : role.getSpan().getTargets()) {
		anchor.add(term.getId());
	    }
	    roleObj.append("anchor", anchor);
	    // External references
	    externalRefs = role.getExternalRefs();
	    if (!externalRefs.isEmpty()) {
		List<DBObject> externalRefObjs = new ArrayList<DBObject>();
		for (ExternalRef extRef : externalRefs) {
		    DBObject externalRefObj = this.map(extRef);
		    externalRefObjs.add(externalRefObj);
		}
		roleObj.append("external_references", externalRefObjs);
	    }
	    roleObjs.add(roleObj);
	}
	predicateObj.append("roles", roleObjs);
	
	return predicateObj;
    }

    private DBObject map(ExternalRef extRef) {
	BasicDBObject extRefObj = new BasicDBObject();
	extRefObj.append("resource", extRef.getResource());
	extRefObj.append("reference", extRef.getReference());
	if (extRef.hasConfidence()) {
	    extRefObj.append("confidence", (Float) extRef.getConfidence());
	}
	if (extRef.hasExternalRef()) {
	    extRefObj.append("external_reference", this.map(extRef.getExternalRef()));
	}
	return extRefObj;
    }

    public KAFDocument getNaf(Integer sessionId, String docId) throws Exception
    {
	return this.getNaf(sessionId, docId, "all", "D", null);
    }

    public KAFDocument getNaf(Integer sessionId, String docId, String layerName) throws Exception
    {
	return this.getNaf(sessionId, docId, layerName, "D", null);
    }

    public KAFDocument getNaf(Integer sessionId, String docId, String layerName, String granularity, Integer part) throws Exception
    {
	if (!this.validLayerName(layerName)) {
	    return null;
	}

	KAFDocument naf = new KAFDocument("en", "v1");
	HashMap<String, WF> wfIndex = new HashMap<String, WF>();
	HashMap<String, Term> termIndex = new HashMap<String, Term>();

	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBObject nafObj;


	// Raw text
	this.queryRawTextLayer(sessionId, docId, naf);
	if (layerName.equals("raw")) return naf;

	// Text
	nafObj = this.textColl.findOne(query);
	if (nafObj == null) throw new Exception("Can not find the requested NAF document in the database."); 
	for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
	    this.getWf(mongoAnnotation, naf, wfIndex);
	}
	if (layerName.equals("text")) return naf;

	// Terms
	nafObj = this.termsColl.findOne(query);
	if (nafObj == null) throw new Exception("Can not find the requested NAF document in the database."); 
	for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
	    this.getTerm(mongoAnnotation, naf, termIndex, wfIndex);
	}
	if (layerName.equals("terms")) return naf;

	// Entities
	if (layerName.equals("entities") || layerName.equals("all")) {
	    nafObj = this.entitiesColl.findOne(query);
	    if (nafObj == null) throw new Exception("Can not find the requested NAF document in the database."); 
	    for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		this.getEntity(mongoAnnotation, naf, termIndex);
	    }
	    if (layerName.equals("entities")) return naf;
	}

	// Deps
	if (layerName.equals("deps") || layerName.equals("all")) {
	    nafObj = this.depsColl.findOne(query);
	    if (nafObj != null) { 
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getDep(mongoAnnotation, naf, termIndex);
		}
	    }
	    if (layerName.equals("deps")) return naf;
	}

	// Constituents
	if (layerName.equals("constituents") || layerName.equals("all")) {
	    nafObj = this.constituentsColl.findOne(query);
	    if (nafObj != null) { 
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getTree(mongoAnnotation, naf, termIndex);
		}
	    }
	    if (layerName.equals("constituents")) return naf;
	}

	// Chunks
	if (layerName.equals("chunks") || layerName.equals("all")) {
	    nafObj = this.chunksColl.findOne(query);
	    if (nafObj != null) { 
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getChunk(mongoAnnotation, naf, termIndex);
		}
	    }
	    if (layerName.equals("chunks")) return naf;
	}
	
	// Coreferences
	if (layerName.equals("coreferences") || layerName.equals("all")) {
	    nafObj = this.corefsColl.findOne(query);
	    if (nafObj != null) { 
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getCoref(mongoAnnotation, naf, termIndex);
		}
	    }
	    if (layerName.equals("coreferences")) return naf;
	}

	// Opinions
	if (layerName.equals("opinions") || layerName.equals("all")) {
	    nafObj = this.opinionsColl.findOne(query);
	    if (nafObj != null) { 
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getOpinion(mongoAnnotation, naf, termIndex);
		}
	    }
	    if (layerName.equals("opinions")) return naf;
	}

	// SRL
	if (layerName.equals("srl") || layerName.equals("all")) {
	    nafObj = this.srlColl.findOne(query);
	    if (nafObj != null) { 
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getPredicate(mongoAnnotation, naf, termIndex);
		}
	    }
	    if (layerName.equals("srl")) return naf;
	}

	return naf;
    }

    public boolean validLayerName(String layerName)
    {
	return layerName.equals("all")
	    || layerName.equals("raw")
	    || layerName.equals("text")
	    || layerName.equals("terms")
	    || layerName.equals("entities")
	    || layerName.equals("deps")
	    || layerName.equals("constituents")
	    || layerName.equals("chunks")
	    || layerName.equals("coreferences")
	    || layerName.equals("opinions")
	    || layerName.equals("srl");
    }


    /* PRIVATE METHODS */

    private void getWf(DBObject mongoWf, KAFDocument naf, HashMap<String, WF> wfIndex)
    {
	WF wf = naf.newWF((String) mongoWf.get("id"), (String) mongoWf.get("form"), (Integer) mongoWf.get("sent"));
	if (mongoWf.containsField("para")) wf.setPara((Integer) mongoWf.get("para"));
	if (mongoWf.containsField("page")) wf.setPage((Integer) mongoWf.get("page"));
	if (mongoWf.containsField("offset")) wf.setOffset((Integer) mongoWf.get("offset"));
	if (mongoWf.containsField("length")) wf.setLength((Integer) mongoWf.get("length"));
	if (mongoWf.containsField("xpath")) wf.setXpath((String) mongoWf.get("xpath"));
	wfIndex.put(wf.getId(), wf);
    }

    private void getTerm(DBObject mongoTerm, KAFDocument naf, HashMap<String, Term> termIndex, HashMap<String, WF> wfIndex)
    {
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
	if (mongoTerm.containsField("sentiment")) {
	    DBObject mongoSentiment = (DBObject) mongoTerm.get("sentiment");
	    Term.Sentiment sentiment = naf.newSentiment();
	    if (mongoSentiment.containsField("resource"))
		sentiment.setResource((String) mongoSentiment.get("resource"));
	    if (mongoSentiment.containsField("polarity"))
		sentiment.setPolarity((String) mongoSentiment.get("polarity"));
	    if (mongoSentiment.containsField("strength"))
		sentiment.setStrength((String) mongoSentiment.get("strength"));
	    if (mongoSentiment.containsField("subjectivity"))
		sentiment.setSubjectivity((String) mongoSentiment.get("subjectivity"));
	    if (mongoSentiment.containsField("sentimentSemanticType"))
		sentiment.setSentimentSemanticType((String) mongoSentiment.get("sentimentSemanticType"));
	    if (mongoSentiment.containsField("sentimentModifier"))
		sentiment.setSentimentModifier((String) mongoSentiment.get("sentimentModifier"));
	    if (mongoSentiment.containsField("sentimentMarker"))
		sentiment.setSentimentMarker((String) mongoSentiment.get("sentimentMarker"));
	    if (mongoSentiment.containsField("sentimentProductFeature"))
		sentiment.setSentimentProductFeature((String) mongoSentiment.get("sentimentProductFeature"));
	    term.setSentiment(sentiment);
	}
	if (mongoTerm.containsField("components")) {
	    List<DBObject> mongoComponents = (List<DBObject>) mongoTerm.get("components");
	    for (DBObject mongoComponent : mongoComponents) {
		Term component = naf.newTerm((String) mongoComponent.get("id"), naf.newWFSpan(), true);
		if (mongoComponent.containsField("type")) component.setType((String) mongoComponent.get("type"));
		if (mongoComponent.containsField("lemma")) component.setLemma((String) mongoComponent.get("lemma"));
		if (mongoComponent.containsField("pos")) component.setPos((String) mongoComponent.get("pos"));
		if (mongoComponent.containsField("morphofeat")) component.setMorphofeat((String) mongoComponent.get("morphofeat"));
		if (mongoComponent.containsField("case")) component.setCase((String) mongoComponent.get("case"));
		boolean isHead = mongoComponent.containsField("head") &&
		    ((String) mongoComponent.get("head")).equals("yes");
		component.addExternalRefs(this.externalRefsMongo2Naf(mongoComponent, naf));
		term.addComponent(component, isHead);
	    }
	}
	term.addExternalRefs(this.externalRefsMongo2Naf(mongoTerm, naf));
	termIndex.put(term.getId(), term);	
    }

    private void getEntity(DBObject mongoEntity, KAFDocument naf, HashMap<String, Term> termIndex)
    {
	Span<Term> terms = this.termSpanMongo2Naf(mongoEntity, termIndex);
	List<Span<Term>> termSpans = new ArrayList<Span<Term>>();
	termSpans.add(terms);
	Entity entity = naf.newEntity((String) mongoEntity.get("id"), termSpans);
	if (mongoEntity.containsField("type")) entity.setType((String) mongoEntity.get("type"));
	entity.addExternalRefs(this.externalRefsMongo2Naf(mongoEntity, naf));
    }

    private void getDep(DBObject mongoDep, KAFDocument naf, HashMap<String, Term> termIndex)
    {
	Term from = termIndex.get((String) mongoDep.get("from"));
	Term to = termIndex.get((String) mongoDep.get("to"));
	String rfunc = (String) mongoDep.get("rfunc");
	Dep dep = naf.newDep(from, to, rfunc);
	if (mongoDep.containsField("case")) {
	    dep.setCase((String) mongoDep.get("case"));
	}	
    }

    private void getTree(DBObject mongoTree, KAFDocument naf, HashMap<String, Term> termIndex)
    {
	HashMap<String, TreeNode> treeNodes = new HashMap<String, TreeNode>();
	HashMap<String, Boolean> areRoot = new HashMap<String, Boolean>();
	// Terminals
	for (DBObject mongoTerminal : (List<DBObject>) mongoTree.get("terminals")) {
	    Span<Term> terms = this.termSpanMongo2Naf(mongoTerminal, termIndex);
	    String id = (String) mongoTerminal.get("id");
	    treeNodes.put(id, naf.newTerminal(id, terms));
	    areRoot.put(id, true);
	}
	// NonTerminals
	for (DBObject mongoNonTerminal : (List<DBObject>) mongoTree.get("non_terminals")) {
	    String label = (String) mongoNonTerminal.get("label");
	    String id = (String) mongoNonTerminal.get("id");
	    treeNodes.put(id, naf.newNonTerminal(id, label));
	    areRoot.put(id, true);
	}
	// Edges
	for (DBObject mongoEdge : (List<DBObject>) mongoTree.get("edges")) {
	    String id = (String) mongoEdge.get("id");
	    TreeNode parentNode = treeNodes.get((String) mongoEdge.get("to"));
	    TreeNode childNode = treeNodes.get((String) mongoEdge.get("from"));
	    Boolean isHead = mongoEdge.containsField("head");
	    try {
		((NonTerminal) parentNode).addChild(childNode);
	    } catch (Exception e) {}
	    areRoot.put((String) mongoEdge.get("from"), false);
	    childNode.setEdgeId(id);
	    if (isHead) ((NonTerminal) childNode).setHead(true);
	}
	// Constituent objects
	for (Map.Entry<String, Boolean> isRoot : areRoot.entrySet()) {
	    if (isRoot.getValue()) {
		TreeNode rootNode = treeNodes.get(isRoot.getKey());
		naf.newConstituent(rootNode);
	    }
	}	
    }

    private void getChunk(DBObject mongoChunk, KAFDocument naf, HashMap<String, Term> termIndex)
    {
	Span<Term> terms = this.termSpanMongo2Naf(mongoChunk, termIndex);
	String id = (String) mongoChunk.get("id");
	Chunk chunk = naf.newChunk(id, terms);
	if (mongoChunk.containsField("phrase")) {
	    chunk.setPhrase((String) mongoChunk.get("phrase"));
	}
	if (mongoChunk.containsField("case")) {
	    chunk.setCase((String) mongoChunk.get("case"));
	}	
    }

    private void getCoref(DBObject mongoCoref, KAFDocument naf, HashMap<String, Term> termIndex)
    {
	List<BasicDBList> mentionObjs = (List<BasicDBList>) mongoCoref.get("anchor");
	List<Span<Term>> mentions = new ArrayList<Span<Term>>();
	for (BasicDBList termIds : mentionObjs) {
	    Span<Term> terms = KAFDocument.newTermSpan();
	    for (int i = 0; i < termIds.size(); i++) {
		String tid = (String) termIds.get(i);
		Term term = termIndex.get(tid);		   
		terms.addTarget(term);
	    }
	    mentions.add(terms);
	}
	String id = (String) mongoCoref.get("id");
	Coref coref = naf.newCoref(id, mentions);	
    }

    private void getOpinion(DBObject mongoOpinion, KAFDocument naf, HashMap<String, Term> termIndex)
    {
	DBObject mongoOpHolder = (DBObject) mongoOpinion.get("opinion_holder");
	DBObject mongoOpTarget = (DBObject) mongoOpinion.get("opinion_target");
	DBObject mongoOpExpression = (DBObject) mongoOpinion.get("opinion_expression");
	String id = (String) mongoOpinion.get("id");
	Opinion opinion = naf.newOpinion(id);
	// Opinion Holder
	Span<Term> terms = this.termSpanMongo2Naf(mongoOpHolder, termIndex);
	Opinion.OpinionHolder opHolder = opinion.createOpinionHolder(terms);
	if (mongoOpHolder.containsField("type")) {
	    opHolder.setType((String) mongoOpHolder.get("type"));
	}
	// Opinion Target
	terms = this.termSpanMongo2Naf(mongoOpTarget, termIndex);
	Opinion.OpinionTarget opTarget = opinion.createOpinionTarget(terms);
	// Opinion Expression
	terms = this.termSpanMongo2Naf(mongoOpExpression, termIndex);
	Opinion.OpinionExpression opExpression = opinion.createOpinionExpression(terms);
	if (mongoOpExpression.containsField("polarity"))
	    opExpression.setPolarity((String) mongoOpExpression.get("polarity"));
	if (mongoOpExpression.containsField("strength"))
	    opExpression.setStrength((String) mongoOpExpression.get("strength"));
	if (mongoOpExpression.containsField("subjectivity"))
	    opExpression.setSubjectivity((String) mongoOpExpression.get("subjectivity"));
	if (mongoOpExpression.containsField("sentiment_semantic_type"))
	    opExpression.setSentimentSemanticType((String) mongoOpExpression.get("sentiment_semantic_type"));
	if (mongoOpExpression.containsField("sentiment_product_feature"))
	    opExpression.setSentimentProductFeature((String) mongoOpExpression.get("sentiment_product_feature"));	
    }

    private void getPredicate(DBObject mongoPredicate, KAFDocument naf, HashMap<String, Term> termIndex)
    {
	String id = (String) mongoPredicate.get("id");
	BasicDBList termIds = (BasicDBList) mongoPredicate.get("anchor");
	Span<Term> terms = this.termSpanMongo2Naf(mongoPredicate, termIndex);
	Predicate predicate = naf.newPredicate(id, terms);
	if (mongoPredicate.containsField("uri")) {
	    predicate.setUri((String) mongoPredicate.get("uri"));
	}
	if (mongoPredicate.containsField("confidence")) {
	    predicate.setConfidence((float) mongoPredicate.get("confidence"));
	}
	for (DBObject mongoRole : (List<DBObject>) mongoPredicate.get("roles")) {
	    String roleId = (String) mongoRole.get("id");
	    String semRole = (String) mongoRole.get("sem_role");
	    terms = this.termSpanMongo2Naf(mongoPredicate, termIndex);
	    Predicate.Role role = naf.newRole(roleId, predicate, semRole, terms);
	    role.addExternalRefs(this.externalRefsMongo2Naf(mongoRole, naf));
	    predicate.addRole(role);
	}
	predicate.addExternalRefs(this.externalRefsMongo2Naf(mongoPredicate, naf));	
    }

    private void queryRawTextLayer(Integer sessionId, String docId, KAFDocument naf)
    {
	String id = docId + "_" + sessionId;
	DBObject query = new BasicDBObject("_id", id);
	DBObject rawTextObj = this.rawColl.findOne(query);
	if (rawTextObj != null) {
	    String raw = (String) rawTextObj.get("raw");
	    naf.setRawText(raw);
	}
    }

    /*
    private HashMap<String, WF> queryTextLayer(Integer sessionId, String docId, KAFDocument naf, String granularity, Integer part)
    {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBObject nafObj = this.textColl.findOne(query);
	List<DBObject> mongoWfs = (List<DBObject>) nafObj.get("annotations");
	HashMap<String, WF> wfIndex = new HashMap<String, WF>();
	for (DBObject mongoWf : mongoWfs) {

	}
	return wfIndex;
    }

    private HashMap<String, Term> queryTermsLayer(Integer sessionId, String docId, KAFDocument naf, HashMap<String, WF> wfIndex, String granularity, Integer part) {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBCursor cursor = this.termsColl.find(query);
	HashMap<String, Term> termIndex = new HashMap<String, Term>();
	while (cursor.hasNext()) {
	    List<DBObject> mongoTerms = (List<DBObject>) cursor.next().get("annotations");
	    for (DBObject mongoTerm : mongoTerms) {

	    }
	}
	return termIndex;
    }
    
    private HashMap<String, Entity> queryEntitiesLayer(Integer sessionId, String docId, KAFDocument naf, HashMap<String, Term> termIndex, String granularity, Integer part) {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBCursor cursor = this.entitiesColl.find(query);
	HashMap<String, Entity> entityIndex = new HashMap<String, Entity>();
	while (cursor.hasNext()) {
	    List<DBObject> mongoEntities = (List<DBObject>) cursor.next().get("annotations");
	    for (DBObject mongoEntity : mongoEntities) {

	    }
	}
	return entityIndex;
    }

    private void queryDepsLayer(Integer sessionId, String docId, KAFDocument naf, HashMap<String, Term> termIndex, String granularity, Integer part) {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBCursor cursor = this.depsColl.find(query);
	while (cursor.hasNext()) {
	    List<DBObject> mongoDeps = (List<DBObject>) cursor.next().get("annotations");
	    for (DBObject mongoDep : mongoDeps) {

	    }
	}
    }

    private void queryConstituentsLayer(Integer sessionId, String docId, KAFDocument naf, HashMap<String, Term> termIndex, String granularity, Integer part) {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBCursor cursor = this.constituentsColl.find(query);
	while (cursor.hasNext()) {
	    List<DBObject> mongoTrees = (List<DBObject>) cursor.next().get("annotations");
	    for (DBObject mongoTree : mongoTrees) {

	    }
	}
    }

    private void queryChunksLayer(Integer sessionId, String docId, KAFDocument naf, HashMap<String, Term> termIndex, String granularity, Integer part) {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBCursor cursor = this.chunksColl.find(query);
	while (cursor.hasNext()) {
	    List<DBObject> mongoChunks = (List<DBObject>) cursor.next().get("annotations");
	    for (DBObject mongoChunk : mongoChunks) {

	    }
	}
    }

    /*
    private void queryCoreferencesLayer(Integer sessionId, String docId, KAFDocument naf, HashMap<String, Term> termIndex, String granularity, Integer part) {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBCursor cursor = this.corefsColl.find(query);
	while (cursor.hasNext()) {
	    List<DBObject> mongoCorefs = (List<DBObject>) cursor.next().get("annotations");
	    for (DBObject mongoCoref : mongoCorefs) {

	    }
	}
    }
    

    private void queryOpinionsLayer(Integer sessionId, String docId, KAFDocument naf, HashMap<String, Term> termIndex, String granularity, Integer part) {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBCursor cursor = this.opinionsColl.find(query);
	while (cursor.hasNext()) {
	    List<DBObject> mongoOpinions = (List<DBObject>) cursor.next().get("annotations");
	    for (DBObject mongoOpinion : mongoOpinions) {

	    }
	}
    }

    private void querySrlLayer(Integer sessionId, String docId, KAFDocument naf, HashMap<String, Term> termIndex, String granularity, Integer part) {
	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBCursor cursor = this.srlColl.find(query);
	while (cursor.hasNext()) {
	    List<DBObject> mongoPredicates = (List<DBObject>) cursor.next().get("annotations");
	    for (DBObject mongoPredicate : mongoPredicates) {

	    }
	}
    }
    */

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

    private Span<Term> termSpanMongo2Naf(DBObject obj, Map<String, Term> termIndex)
    {
	BasicDBList termIds = (BasicDBList) obj.get("anchor");
	Span<Term> terms = KAFDocument.newTermSpan();
	for (int i = 0; i < termIds.size(); i++) {
	    String tid = (String) termIds.get(i);
	    Term term = termIndex.get(tid);		   
	    terms.addTarget(term);
	}
	return terms;
    }

    private List<ExternalRef> externalRefsMongo2Naf(DBObject obj, KAFDocument naf)
    {
	List<ExternalRef> externalRefs = new ArrayList<ExternalRef>();
	List<DBObject> mongoExternalRefs = (List<DBObject>) obj.get("external_references");
	if (mongoExternalRefs != null) {
	    for (DBObject mongoExternalRef : mongoExternalRefs) {
		externalRefs.add(this.externalRefMongo2Naf(mongoExternalRef, naf));
	    }
	}
	return externalRefs;
    }

    private ExternalRef externalRefMongo2Naf(DBObject mongoExtRef, KAFDocument naf)
    {
	String resource = (String) mongoExtRef.get("resource");
	String reference = (String) mongoExtRef.get("reference");
	ExternalRef extRef = naf.newExternalRef(resource, reference);
	if (mongoExtRef.containsField("confidence")) {
	    extRef.setConfidence(new Float((Double) mongoExtRef.get("confidence")));
	}
	if (mongoExtRef.containsField("external_reference"))
	    extRef.setExternalRef(this.externalRefMongo2Naf((DBObject) mongoExtRef.get("external_reference"), naf));
	return extRef;
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
