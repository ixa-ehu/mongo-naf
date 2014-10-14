package ixa.storm;

import ixa.kaflib.*;
import ixa.kaflib.KAFDocument.LinguisticProcessor;
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
    private DBCollection lpColl;
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
    private DBCollection factualityColl;
    private DBCollection timeExpressionsColl;
    private DBCollection temporalRelationsColl;
    private DBCollection causalRelationsColl;

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
	this.lpColl = this.db.getCollection("linguisticProcessors");
	this.rawColl = this.db.getCollection("raw");
	this.textColl = this.db.getCollection("text");
	this.termsColl = this.db.getCollection("terms");
	this.entitiesColl = this.db.getCollection("entities");
	this.depsColl = this.db.getCollection("deps");
	this.constituentsColl = this.db.getCollection("constituency");
	this.chunksColl = this.db.getCollection("chunks");
	this.corefsColl = this.db.getCollection("coreferences");
	this.opinionsColl = this.db.getCollection("opinions");
	this.srlColl = this.db.getCollection("srl");
	this.factualityColl = this.db.getCollection("factualitylayer");
	this.timeExpressionsColl = this.db.getCollection("timeExpressions");
	this.temporalRelationsColl = this.db.getCollection("temporalRelations");
	this.causalRelationsColl = this.db.getCollection("causalRelations");
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
	this.lpColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1).append("name", 1));
	this.rawColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.textColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.termsColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.entitiesColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.depsColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.constituentsColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.chunksColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.corefsColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.opinionsColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.srlColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.factualityColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.timeExpressionsColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.temporalRelationsColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
	this.causalRelationsColl.createIndex(new BasicDBObject("session_id", 1).append("doc_id", 1));
    }

    public void drop() {
	this.db.dropDatabase();
    }

    public void removeDoc(String docId) {
	String idField = "doc_id";
	this.lpColl.remove(new BasicDBObject(idField, docId));
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
	this.factualityColl.remove(new BasicDBObject(idField, docId));
	this.timeExpressionsColl.remove(new BasicDBObject(idField, docId));
	this.temporalRelationsColl.remove(new BasicDBObject(idField, docId));
	this.causalRelationsColl.remove(new BasicDBObject(idField, docId));
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
	this.insertLinguisticProcessors(docId, sessionId, naf);
	this.insertLayer(docId, sessionId, naf, "raw", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "text", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "terms", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "entities", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "deps", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "constituency", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "chunks", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "coreferences", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "opinions", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "srl", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "factualitylayer", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "timeExpressions", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "temporalRelations", paragraph, sentence);
	this.insertLayer(docId, sessionId, naf, "causalRelations", paragraph, sentence);
    }

    public void insertLinguisticProcessors(String docId, Integer sessionId, KAFDocument naf) {
	List<String> existingLps = this.getLinguisticProcessorNames(docId, sessionId);
	List<LinguisticProcessor> lps = naf.getLinguisticProcessorList();
	for (LinguisticProcessor lp : lps) {
	    if (!existingLps.contains(lp.getName())) {
		this.insertLinguisticProcessor(docId, sessionId, lp);
	    }
	}
    }

    public void insertLinguisticProcessor(String docId, Integer sessionId, LinguisticProcessor lp) {
	BasicDBObject doc = new BasicDBObject()
	    .append("session_id", sessionId)
	    .append("doc_id", docId)
	    .append("name", lp.getName())
	    .append("layer", lp.getLayer());
	String id = sessionId + "_" + docId + "_" + lp.getName();
	doc.append("_id", id);
	if (lp.hasTimestamp()) {
	    doc.append("timestamp", lp.getTimestamp());
	}
	if (lp.hasBeginTimestamp()) {
	    doc.append("beginTimestamp", lp.getBeginTimestamp());
	}
	if (lp.hasEndTimestamp()) {
	    doc.append("endTimestamp", lp.getEndTimestamp());
	}
	if (lp.hasVersion()) {
	    doc.append("version", lp.getVersion());
	}
	try {
	    this.lpColl.save(doc);
	} catch(MongoException e) {
	    System.out.println("Error storing a LP.");
	}
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
	//	System.out.println("Insert: " + layerName);

	if (layerName.equals("raw")) {
	    String layer = naf.getRawText();
	    this.insertRawText(layer, sessionId, docId);
	}
	else {
	    List<DBObject> annDBObjs = new ArrayList<DBObject>();
	    DBCollection docCollection = null;
	    if (layerName.equals("text")) {
		for (WF annotation : naf.getWFs()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.textColl;
	    }
	    if (layerName.equals("terms")) {
	        for (Term annotation : naf.getTerms()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.termsColl;
	    }
	    if (layerName.equals("entities")) {
		for (Entity annotation : naf.getEntities()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.entitiesColl;
	    }
	    if (layerName.equals("deps")) {
	        for (Dep annotation : naf.getDeps()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.depsColl;
	    }
	    if (layerName.equals("constituency")) {
	        for (Tree annotation : naf.getConstituents()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.constituentsColl;
	    }
	    if (layerName.equals("chunks")) {
	        for (Chunk annotation : naf.getChunks()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.chunksColl;
	    }
	    if (layerName.equals("coreferences")) {
	        for (Coref annotation : naf.getCorefs()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.corefsColl;
	    }
	    if (layerName.equals("opinions")) {
	        for (Opinion annotation : naf.getOpinions()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.opinionsColl;
	    }
	    if (layerName.equals("srl")) { 
	        for (Predicate annotation : naf.getPredicates()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.srlColl;
	    }
	    if (layerName.equals("factualitylayer")) {
	        for (Factuality annotation : naf.getFactualities()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.factualityColl;
	    }
	    if (layerName.equals("timeExpressions")) { 
		for (Timex3 annotation : naf.getTimeExs()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.timeExpressionsColl;
	    }
	    if (layerName.equals("temporalRelations")) { 
		for (TLink annotation : naf.getTLinks()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.temporalRelationsColl;
	    }
	    if (layerName.equals("causalRelations")) { 
		for (CLink annotation : naf.getCLinks()) {
		    annDBObjs.add(this.map(annotation));
		}
		docCollection = this.causalRelationsColl;
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
	    .append("session_id", sessionId)
	    .append("doc_id", docId)
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
	    collection.save(doc);
	} catch(MongoException e) {
	    System.out.println("Error storing a layer.");
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
	if (coref.hasType()) corefObj.append("type", coref.getType());
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
	if (opHolder != null) {
	    BasicDBObject opHolderObj = new BasicDBObject();
	    if (opHolder.hasType()) opHolderObj.append("type", opHolder.getType());
	    List<String> anchor = new ArrayList<String>();
	    for (Term term : opHolder.getSpan().getTargets()) {
		anchor.add(term.getId());
	    }
	    opHolderObj.append("anchor", anchor);
	    opinionObj.append("opinion_holder", opHolderObj);
	}
	// OpinionTarget
	Opinion.OpinionTarget opTarget = opinion.getOpinionTarget();
	if (opTarget != null) {
	    BasicDBObject opTargetObj = new BasicDBObject();
	    List<String> anchor = new ArrayList<String>();
	    for (Term term : opTarget.getSpan().getTargets()) {
		anchor.add(term.getId());
	    }
	    opTargetObj.append("anchor", anchor);
	    opinionObj.append("opinion_target", opTargetObj);
	}
	// OpinionExpression
	Opinion.OpinionExpression opExpression = opinion.getOpinionExpression();
	if (opExpression != null) {
	    BasicDBObject opExpressionObj = new BasicDBObject();
	    if (opExpression.hasPolarity()) opExpressionObj.append("polarity", opExpression.getPolarity());
	    if (opExpression.hasStrength()) opExpressionObj.append("strength", opExpression.getStrength());
	    if (opExpression.hasSubjectivity()) opExpressionObj.append("subjectivity", opExpression.getSubjectivity());
	    if (opExpression.hasSentimentSemanticType()) opExpressionObj.append("sentiment_semantic_type", opExpression.getSentimentSemanticType());
	    if (opExpression.hasSentimentProductFeature()) opExpressionObj.append("sentiment_product_feature", opExpression.getSentimentProductFeature());
	    List<String> anchor = new ArrayList<String>();
	    for (Term term : opExpression.getSpan().getTargets()) {
		anchor.add(term.getId());
	    }
	    opExpressionObj.append("anchor", anchor);
	    opinionObj.append("opinion_expression", opExpressionObj);
	}

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

    private DBObject map(Factuality factuality) {
	// Linginfo
	BasicDBObject factualityObj = new BasicDBObject("id", factuality.getId()).
	    append("prediction", factuality.getPrediction());
	if (factuality.hasConfidence()) {
	    factualityObj.append("confidence", factuality.getConfidence());
	}
	return factualityObj;
    }

    private DBObject map(Timex3 timex3) {
	// Linginfo
	BasicDBObject timex3Obj = new BasicDBObject("id", timex3.getId()).
	    append("type", timex3.getType());
	if (timex3.hasBeginPoint()) {
	    timex3Obj.append("beginPoint", timex3.getBeginPoint().getId());
	}
	if (timex3.hasEndPoint()) {
	    timex3Obj.append("endPoint", timex3.getEndPoint().getId());
	}
	if (timex3.hasQuant()) {
	    timex3Obj.append("quant", timex3.getQuant());
	}
	if (timex3.hasFreq()) {
	    timex3Obj.append("freq", timex3.getFreq());
	}
	if (timex3.hasFunctionInDocument()) {
	    timex3Obj.append("functionInDocument", timex3.getFunctionInDocument());
	}
	if (timex3.hasTemporalFunction()) {
	    String tempFun = timex3.getTemporalFunction() ? "true" : "false";
	    timex3Obj.append("temporalFunction", tempFun);
	}
	if (timex3.hasValue()) {
	    timex3Obj.append("value", timex3.getValue());
	}
	if (timex3.hasValueFromFunction()) {
	    timex3Obj.append("valueFromFunction", timex3.getValueFromFunction());
	}
	if (timex3.hasMod()) {
	    timex3Obj.append("mod", timex3.getMod());
	}
	if (timex3.hasAnchorTimeId()) {
	    timex3Obj.append("anchorTimeId", timex3.getAnchorTimeId());
	}
	if (timex3.hasComment()) {
	    timex3Obj.append("comment", timex3.getComment());
	}
	if (timex3.hasSpan()) {
	    Span<WF> span = timex3.getSpan();
	    List<String> spanObj = new ArrayList<String>();
	    for (WF wf : span.getTargets()) {
		spanObj.add(wf.getId());
	    }
	    timex3Obj.append("anchor", spanObj);
	}

	return timex3Obj;
    }

    private DBObject map(TLink tLink) {
	String fromType = (tLink.getFrom() instanceof Coref) ? "event" : "timex";
	String toType = (tLink.getTo() instanceof Coref) ? "event" : "timex";
	BasicDBObject tLinkObj = new BasicDBObject("id", tLink.getId()).
	    append("from", tLink.getFrom().getId()).
	    append("to", tLink.getTo().getId()).
	    append("fromType", fromType).
	    append("toType", toType).
	    append("relType", tLink.getRelType());
	return tLinkObj;
    }

    private DBObject map(CLink cLink) {
	BasicDBObject cLinkObj = new BasicDBObject("id", cLink.getId()).
	    append("from", cLink.getFrom().getId()).
	    append("to", cLink.getTo().getId());
	if (cLink.hasRelType()) {
	    cLinkObj.append("relType", cLink.getRelType());
	}
	return cLinkObj;
    }

    private DBObject map(ExternalRef extRef) {
	BasicDBObject extRefObj = new BasicDBObject();
	extRefObj.append("resource", extRef.getResource());
	extRefObj.append("reference", extRef.getReference());
	if (extRef.hasConfidence()) {
	    extRefObj.append("confidence", (Float) extRef.getConfidence());
	}
	if (extRef.hasExternalRef()) {
	    for (ExternalRef subExtRef : extRef.getExternalRefs()) {
		extRefObj.append("external_reference", this.map(subExtRef));
	    }
	}
	return extRefObj;
    }

    public KAFDocument getNaf(Integer sessionId, String docId) throws Exception
    {
	List<String> layers = new ArrayList<String>();
	layers.add("all");
	return this.getNaf(sessionId, docId, layers, "D", null);
    }

    public KAFDocument getNaf(Integer sessionId, String docId, List<String> layerNames) throws Exception
    {
	return this.getNaf(sessionId, docId, layerNames, "D", null);
    }

    public KAFDocument getNaf(Integer sessionId, String docId, List<String> layerNames, String granularity, Integer part) throws Exception
    {
	//System.out.println("Get: " + layerName);

	/*
	if (!this.validLayerName(layerName)) {
	    return null;
	}
	*/
	KAFDocument naf = new KAFDocument("en", "v1");

	this.getLinguisticProcessors(sessionId, docId, naf);

	HashMap<String, WF> wfIndex = new HashMap<String, WF>();
	HashMap<String, Term> termIndex = new HashMap<String, Term>();
	HashMap<String, Coref> corefIndex = new HashMap<String, Coref>();
	HashMap<String, Timex3> timexIndex = new HashMap<String, Timex3>();

	BasicDBObject query = this.createQuery(sessionId, docId, granularity, part);
	DBObject nafObj;


	// Raw text
	this.queryRawTextLayer(sessionId, docId, naf);
	layerNames.remove("raw");
	if (layerNames.isEmpty()) return naf;
	
	// Text
	nafObj = this.textColl.findOne(query);
	if (nafObj != null) {
	    for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		this.getWf(mongoAnnotation, naf, wfIndex);
	    }
	}
	layerNames.remove("text");
	if (layerNames.isEmpty()) return naf;

	// Terms
	nafObj = this.termsColl.findOne(query);
	if (nafObj != null) {
	    for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		this.getTerm(mongoAnnotation, naf, termIndex, wfIndex);
	    }
	}
	layerNames.remove("terms");
	if (layerNames.isEmpty()) return naf;

	// Entities
	if (layerNames.contains("entities") || layerNames.get(0).equals("all")) {
	    nafObj = this.entitiesColl.findOne(query);
	    if (nafObj != null) {
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getEntity(mongoAnnotation, naf, termIndex);
		}
	    }
	    layerNames.remove("entities");
	    if (layerNames.isEmpty()) return naf;
	}

	// Deps
	if (layerNames.contains("deps") || layerNames.get(0).equals("all")) {
	    nafObj = this.depsColl.findOne(query);
	    if (nafObj != null) {
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getDep(mongoAnnotation, naf, termIndex);
		}
	    }
	    layerNames.remove("deps");
	    if (layerNames.isEmpty()) return naf;
	}

	// Constituents
	if (layerNames.contains("constituency") || layerNames.get(0).equals("all")) {
	    nafObj = this.constituentsColl.findOne(query);
	    if (nafObj != null) {
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getTree(mongoAnnotation, naf, termIndex);
		}
	    }
	    layerNames.remove("constituency");
	    if (layerNames.isEmpty()) return naf;
	}

	// Chunks
	if (layerNames.contains("chunks") || layerNames.get(0).equals("all")) {
	    nafObj = this.chunksColl.findOne(query);
	    if (nafObj != null) {
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getChunk(mongoAnnotation, naf, termIndex);
		}
	    }
	    layerNames.remove("chunks");
	    if (layerNames.isEmpty()) return naf;
	}
	
	// Coreferences
	if (layerNames.contains("coreferences") || layerNames.get(0).equals("all")) {
	    nafObj = this.corefsColl.findOne(query);
	    if (nafObj != null) {
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getCoref(mongoAnnotation, naf, corefIndex, termIndex);
		}
	    }
	    layerNames.remove("coreferences");
	    if (layerNames.isEmpty()) return naf;
	}

	// Opinions
	if (layerNames.contains("opinions") || layerNames.get(0).equals("all")) {
	    nafObj = this.opinionsColl.findOne(query);
	    if (nafObj != null) {
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getOpinion(mongoAnnotation, naf, termIndex);
		}
	    }
	    layerNames.remove("opinions");
	    if (layerNames.isEmpty()) return naf;
	}

	// SRL
	if (layerNames.contains("srl") || layerNames.get(0).equals("all")) {
	    nafObj = this.srlColl.findOne(query);
	    if (nafObj != null) {
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getPredicate(mongoAnnotation, naf, termIndex);
		}
	    }
	    layerNames.remove("srl");
	    if (layerNames.isEmpty()) return naf;
	}

	// Factuality layer
	if (layerNames.contains("factualitylayer") || layerNames.get(0).equals("all")) {
	    nafObj = this.factualityColl.findOne(query);
	    if (nafObj != null) {
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getFactuality(mongoAnnotation, naf, wfIndex);
		}
	    }
	    layerNames.remove("factualitylayer");
	    if (layerNames.isEmpty()) return naf;
	}

	// TimeExpressions layer
	if (layerNames.contains("timeExpressions") || layerNames.get(0).equals("all")) {
	    nafObj = this.timeExpressionsColl.findOne(query);
	    if (nafObj != null) {
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getTimex3(mongoAnnotation, naf, timexIndex, wfIndex, termIndex);
		}
	    }
	    layerNames.remove("timeExpressions");
	    if (layerNames.isEmpty()) return naf;
	}

	// TemporalRelations layer
	if (layerNames.contains("temporalRelations") || layerNames.get(0).equals("all")) {
	    nafObj = this.temporalRelationsColl.findOne(query);
	    if (nafObj != null) {
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getTLink(mongoAnnotation, naf, corefIndex, timexIndex);
		}
	    }
	    layerNames.remove("temporalRelations");
	    if (layerNames.isEmpty()) return naf;
	}

	// CausalRelations layer
	if (layerNames.contains("causalRelations") || layerNames.get(0).equals("all")) {
	    nafObj = this.causalRelationsColl.findOne(query);
	    if (nafObj != null) {
		for (DBObject mongoAnnotation : (List<DBObject>) nafObj.get("annotations")) {
		    this.getCLink(mongoAnnotation, naf, corefIndex);
		}
	    }
	    layerNames.remove("causalRelations");
	    if (layerNames.isEmpty()) return naf;
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
	    || layerName.equals("constituency")
	    || layerName.equals("chunks")
	    || layerName.equals("coreferences")
	    || layerName.equals("opinions")
	    || layerName.equals("srl")
	    || layerName.equals("factualitylayer")
	    || layerName.equals("timeExpressions")
	    || layerName.equals("temporalRelations")
	    || layerName.equals("causalRelations");
    }

    public List<String> getLinguisticProcessorNames(String docId, Integer sessionId) {
	DBObject query = new BasicDBObject("session_id", sessionId)
	    .append("doc_id", docId);
        return this.lpColl.distinct("name", query);
    }

    public void getLinguisticProcessors(Integer sessionId, String docId, KAFDocument naf) {
	DBObject query = new BasicDBObject("session_id", sessionId)
	    .append("doc_id", docId);
        List<DBObject> mongoLps = this.lpColl.find(query).toArray();
	for (DBObject mongoLp : mongoLps) {
	    this.getLp(mongoLp, naf);
	}
    }


    /* PRIVATE METHODS */

    private void getLp(DBObject mongoLp, KAFDocument naf) {
	String layer = (String) mongoLp.get("layer");
	String name = (String) mongoLp.get("name");
	if (!naf.linguisticProcessorExists(layer, name)) {
	    LinguisticProcessor newLp = naf.addLinguisticProcessor(layer, name);
	    if (mongoLp.containsField("version")) {
		newLp.setVersion((String) mongoLp.get("version"));
	    }
	    if (mongoLp.containsField("timestamp")) {
		newLp.setTimestamp((String) mongoLp.get("timestamp"));
	    }
	    if (mongoLp.containsField("beginTimestamp")) {
		newLp.setBeginTimestamp((String) mongoLp.get("beginTimestamp"));
	    }
	    if (mongoLp.containsField("endTimestamp")) {
		newLp.setEndTimestamp((String) mongoLp.get("endTimestamp"));
	    }
	}
    }

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

    private void getCoref(DBObject mongoCoref, KAFDocument naf, HashMap<String, Coref> corefIndex, HashMap<String, Term> termIndex)
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
	if (mongoCoref.containsField("type")) {
	    coref.setType((String) mongoCoref.get("type"));
	}
	corefIndex.put(coref.getId(), coref);
    }

    private void getOpinion(DBObject mongoOpinion, KAFDocument naf, HashMap<String, Term> termIndex)
    {
	DBObject mongoOpHolder = (DBObject) mongoOpinion.get("opinion_holder");
	DBObject mongoOpTarget = (DBObject) mongoOpinion.get("opinion_target");
	DBObject mongoOpExpression = (DBObject) mongoOpinion.get("opinion_expression");
	String id = (String) mongoOpinion.get("id");
	Opinion opinion = naf.newOpinion(id);
	// Opinion Holder
	if (mongoOpHolder != null) {
	    Span<Term> terms = this.termSpanMongo2Naf(mongoOpHolder, termIndex);
	    Opinion.OpinionHolder opHolder = opinion.createOpinionHolder(terms);
	    if (mongoOpHolder.containsField("type")) {
		opHolder.setType((String) mongoOpHolder.get("type"));
	    }
	}
	// Opinion Target
	if (mongoOpTarget != null) {
	    Span<Term> terms = this.termSpanMongo2Naf(mongoOpTarget, termIndex);
	    Opinion.OpinionTarget opTarget = opinion.createOpinionTarget(terms);
	}
	// Opinion Expression
	if (mongoOpExpression != null) {
	    Span<Term> terms = this.termSpanMongo2Naf(mongoOpExpression, termIndex);
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

    private void getFactuality(DBObject mongoFactuality, KAFDocument naf, HashMap<String, WF> wfIndex)
    {
	String id = (String) mongoFactuality.get("id");
	String prediction = (String) mongoFactuality.get("prediction");
	Factuality factuality = naf.newFactuality(wfIndex.get(id), prediction);
	if (mongoFactuality.containsField("confidence")) {
	    factuality.setConfidence((Double) mongoFactuality.get("confidence"));
	}
    }

    private void getTimex3(DBObject mongoTimex3, KAFDocument naf, HashMap<String, Timex3> timexIndex, HashMap<String, WF> wfIndex, HashMap<String, Term> termIndex)
    {
	String id = (String) mongoTimex3.get("id");
	String type = (String) mongoTimex3.get("type");
	Timex3 timex3 = naf.newTimex3(id, type);
	if (mongoTimex3.containsField("beginPoint")) {
	    Term beginPoint = termIndex.get((String) mongoTimex3.get("beginPoint"));
	    timex3.setBeginPoint(beginPoint);
	}
	if (mongoTimex3.containsField("endPoint")) {
	    Term endPoint = termIndex.get((String) mongoTimex3.get("endPoint"));
	    timex3.setEndPoint(endPoint);
	}
	if (mongoTimex3.containsField("quant")) {
	    timex3.setQuant((String) mongoTimex3.get("quant"));
	}
	if (mongoTimex3.containsField("freq")) {
	    timex3.setFreq((String) mongoTimex3.get("quant"));
	}
	if (mongoTimex3.containsField("functionInDocument")) {
	    timex3.setFunctionInDocument((String) mongoTimex3.get("functionInDocument"));
	}
	if (mongoTimex3.containsField("temporalFunction")) {
	    Boolean tempFunc = ((String) mongoTimex3.get("temporalFunction")).equals("true");
	    timex3.setTemporalFunction(tempFunc);
	}
	if (mongoTimex3.containsField("value")) {
	    timex3.setValue((String) mongoTimex3.get("value"));
	}
	if (mongoTimex3.containsField("valueFromFunction")) {
	    timex3.setValueFromFunction((String) mongoTimex3.get("valueFromFunction"));
	}
	if (mongoTimex3.containsField("mod")) {
	    timex3.setMod((String) mongoTimex3.get("mod"));
	}
	if (mongoTimex3.containsField("anchorTimeId")) {
	    timex3.setAnchorTimeId((String) mongoTimex3.get("anchorTimeId"));
	}
	if (mongoTimex3.containsField("comment")) {
	    timex3.setComment((String) mongoTimex3.get("comment"));
	}
	if (mongoTimex3.containsField("anchor")) {
	    BasicDBList wfIds = (BasicDBList) mongoTimex3.get("anchor");
	    Span<WF> span = KAFDocument.newWFSpan();
	    for (int i = 0; i < wfIds.size(); i++) {
		span.addTarget(wfIndex.get((String) wfIds.get(i)));
	    }
	    timex3.setSpan(span);
	}
	timexIndex.put(timex3.getId(), timex3);
    }

    private void getTLink(DBObject mongoTLink, KAFDocument naf, HashMap<String, Coref> corefIndex, HashMap<String, Timex3> timexIndex) {
	String id = (String) mongoTLink.get("id");
	String fromId = (String) mongoTLink.get("from");
	String toId = (String) mongoTLink.get("to");
	String fromType = (String) mongoTLink.get("fromType");
	String toType = (String) mongoTLink.get("toType");
	String relType = (String) mongoTLink.get("relType");
	TLinkReferable from = fromType.equals("event") ? corefIndex.get(fromId) : timexIndex.get(fromId);
	TLinkReferable to = toType.equals("event") ? corefIndex.get(toId) : timexIndex.get(toId);
	TLink tLink = naf.newTLink(id, from, to, relType);
    }

    private void getCLink(DBObject mongoCLink, KAFDocument naf, HashMap<String, Coref> corefIndex) {
	String id = (String) mongoCLink.get("id");
	String fromId = (String) mongoCLink.get("from");
	String toId = (String) mongoCLink.get("to");
	String relType = (String) mongoCLink.get("relType");
	Coref from = corefIndex.get(fromId);
	Coref to = corefIndex.get(toId);
	CLink cLink = naf.newCLink(id, from, to);
	if (mongoCLink.containsField("relType")) {
	    cLink.setRelType((String) mongoCLink.get("relType"));
	}
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
	if (mongoExtRef.containsField("external_reference")) {
	    for (DBObject mongoSubExtRef : (List<DBObject>) mongoExtRef.get("external_reference")) {
		extRef.setExternalRef(this.externalRefMongo2Naf(mongoSubExtRef, naf));
	    }
	}
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
