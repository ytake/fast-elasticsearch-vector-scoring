package com.liorkn.elasticsearch.engine;

import com.liorkn.elasticsearch.script.VectorScoreScript;

import java.util.Map;

import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScoreScript;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.index.query.IntervalFilterScript;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Doc;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Field;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Query;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Term;
import org.elasticsearch.search.aggregations.pipeline.MovingFunctionScript;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

/** This {@link ScriptEngine} uses Lucene segment details to implement document scoring based on their similarity with submitted document. */
public class VectorScoringScriptEngine implements ScriptEngine {

    public static final String NAME = "knn";
    private static final String SCRIPT_SOURCE = "binary_vector_score";
    
    @Override
    public String getType() {
        return NAME;
    }
    
    @Override
    public <T> T compile(String scriptName, String scriptSource, ScriptContext<T> context, Map<String, String> params) {
    	if (context.equals(ScoreScript.CONTEXT) == false) {
            throw new IllegalArgumentException(getType() + " scripts cannot be used for context [" + context.name + "]");
        }
    	
    	// we use the script "source" as the script identifier
        if (!SCRIPT_SOURCE.equals(scriptSource)) {
            throw new IllegalArgumentException("Unknown script name " + scriptSource);
        }

    	ScoreScript.Factory factory = VectorScoreScript.VectorScoreScriptFactory::new;
        return context.factoryClazz.cast(factory);
    }
    
    @Override
    public Set<ScriptContext<?>> getSupportedContexts() {
        return Set.of(
            FieldScript.CONTEXT,
            TermsSetQueryScript.CONTEXT,
            NumberSortScript.CONTEXT,
            StringSortScript.CONTEXT,
            IngestScript.CONTEXT,
            AggregationScript.CONTEXT,
            IngestConditionalScript.CONTEXT,
            UpdateScript.CONTEXT,
            BucketAggregationScript.CONTEXT,
            BucketAggregationSelectorScript.CONTEXT,
            SignificantTermsHeuristicScoreScript.CONTEXT,
            TemplateScript.CONTEXT,
            FilterScript.CONTEXT,
            SimilarityScript.CONTEXT,
            SimilarityWeightScript.CONTEXT,
            MovingFunctionScript.CONTEXT,
            ScoreScript.CONTEXT,
            ScriptedMetricAggContexts.InitScript.CONTEXT,
            ScriptedMetricAggContexts.MapScript.CONTEXT,
            ScriptedMetricAggContexts.CombineScript.CONTEXT,
            ScriptedMetricAggContexts.ReduceScript.CONTEXT,
            IntervalFilterScript.CONTEXT
        );
    }
}
