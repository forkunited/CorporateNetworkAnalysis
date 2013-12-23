package corp.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.hadoop.io.Text;

import corp.net.util.JSONUtil;
import corp.net.util.MathUtil;
import edu.stanford.nlp.util.Pair;

public class CorpNetEdgeSource {
	private JSONObject obj;
	
	private String author;
	private String annotationFile;
	
	private List<String> mentions;
	private List<Pair<Integer, Integer>> mentionTokenSpans;
	private List<Integer> mentionSentences;
	
	private Map<Integer, String> sentences;
	private Map<String, Double> p;
	
	public CorpNetEdgeSource(Text jsonText) {
		this(jsonText.toString());
	}
	
	public CorpNetEdgeSource(String jsonText) {
		this(JSONObject.fromObject(jsonText));
	}
	
	public CorpNetEdgeSource(JSONObject obj) {
		this.obj = obj;
		this.author = obj.getString("author");
		this.annotationFile = obj.getString("annotationFile");
		
		JSONArray mentionsObj = obj.getJSONArray("mentions");
		this.mentions = new ArrayList<String>(mentionsObj.size());
		this.mentionTokenSpans = new ArrayList<Pair<Integer, Integer>>(mentionsObj.size());
		this.mentionSentences = new ArrayList<Integer>(mentionsObj.size());
		for (int i = 0; i < mentionsObj.size(); i++) {
			JSONObject mentionObj = mentionsObj.getJSONObject(i);
			this.mentions.add(mentionObj.getString("text"));
			this.mentionTokenSpans.add(new Pair<Integer, Integer>(mentionObj.getInt("tokenStart"), mentionObj.getInt("tokenEnd")));
			this.mentionSentences.add(mentionObj.getInt("sentence"));
		}
		
		JSONArray sentencesObj = obj.getJSONArray("sentences");
		this.sentences = new TreeMap<Integer, String>();
		for (int i = 0; i < sentencesObj.size(); i++) {
			JSONObject sentenceObj = sentencesObj.getJSONObject(i);
			this.sentences.put(sentenceObj.getInt("index"), sentenceObj.getString("text"));		
		}
		
		this.p = JSONUtil.objToDistribution(obj.getJSONObject("p"));
	}
	
	public JSONObject toJSONObject() {
		return this.obj;
	}
	
	public String getAuthor() {
		return this.author;
	}

	public String getAnnotationFile() {
		return this.annotationFile;
	}
	
	public List<String> getMentions() {
		return this.mentions;
	}
	
	public List<Pair<Integer, Integer>> getMentionTokenSpans() {
		return this.mentionTokenSpans;
	}
	
	public List<Integer> getMentionSentences() {
		return this.mentionSentences;
	}
	
	public String getSentenceByIndex(int index) {
		return this.sentences.get(index);
	}
	
	public Map<String, Double> getP() {
		return this.p;
	}
	
	public String getMaxType() {
		return MathUtil.argMaxDistribution(this.p);
	}
	
	public String toHTMLString() {
		StringBuilder str = new StringBuilder();
		str = str.append("Annotation File: ").append(getAnnotationFile()).append("<br />");
		str = str.append("Author: ").append(getAuthor()).append("<br />");
		
		Map<Integer, String> annotatedSentences = new TreeMap<Integer, String>();
		List<Integer> mentionSentences = getMentionSentences();
		List<Pair<Integer, Integer>> mentionTokenSpans = getMentionTokenSpans();
		
		for (int i = 0; i < mentionSentences.size(); i++) {
			String mentionSentence = null;
			int mentionSentenceIndex = mentionSentences.get(i);
			if (annotatedSentences.containsKey(mentionSentenceIndex))
				mentionSentence = annotatedSentences.get(mentionSentenceIndex);
			else
				mentionSentence = getSentenceByIndex(mentionSentenceIndex);

			String[] mentionSentenceTokens = mentionSentence.split("\\s+");
			int mentionStartToken = mentionTokenSpans.get(i).first();
			int mentionEndToken = mentionTokenSpans.get(i).second();
			
			mentionSentenceTokens[mentionStartToken] = "<b>" + mentionSentenceTokens[mentionStartToken];
			mentionSentenceTokens[mentionEndToken-1] = mentionSentenceTokens[mentionEndToken-1] + "</b>";
			
			StringBuilder newMentionSentence = new StringBuilder();
			for (int j = 0; j < mentionSentenceTokens.length; j++)
				newMentionSentence = newMentionSentence.append(mentionSentenceTokens[j]).append(" ");
			annotatedSentences.put(mentionSentenceIndex, newMentionSentence.toString());
		}
		
		str = str.append("Sentences: ").append("<br />");
		for (Entry<Integer, String> entry : annotatedSentences.entrySet()) {
			if (entry.getValue().length() > 3000)
				str = str.append("(Ommitted long source sentence greater than 3000 characters)<br />");
			else
				str = str.append(entry.getValue()).append("<br />");
		}
		
		return str.toString();
	}
}
