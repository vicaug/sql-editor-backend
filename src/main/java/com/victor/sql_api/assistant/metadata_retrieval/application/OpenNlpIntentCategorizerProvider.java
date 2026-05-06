package com.victor.sql_api.assistant.metadata_retrieval.application;

import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

@Component
public class OpenNlpIntentCategorizerProvider {
    private static final Logger log = LoggerFactory.getLogger(OpenNlpIntentCategorizerProvider.class);
    private static final String PT_INTENT_MODEL_CLASSPATH = "nlp/opennlp-pt-intent-doccat-1.0.bin";

    private final DocumentCategorizerME categorizer;

    public OpenNlpIntentCategorizerProvider(@Value("${app.nlp.intent-model-path:}") String intentModelPath) {
        this.categorizer = loadCategorizer(intentModelPath);
    }

    public DocumentCategorizerME getCategorizer() {
        return categorizer;
    }

    private DocumentCategorizerME loadCategorizer(String externalPath) {
        if (externalPath != null && !externalPath.isBlank()) {
            try (InputStream in = Files.newInputStream(Path.of(externalPath.trim()))) {
                DoccatModel model = new DoccatModel(in);
                log.info("Modelo Doccat OpenNLP carregado via application.properties: {}", externalPath.trim());
                return new DocumentCategorizerME(model);
            } catch (Exception ex) {
                log.warn("Falha ao carregar modelo Doccat OpenNLP via app.nlp.intent-model-path={}. Motivo: {}", externalPath.trim(), ex.getMessage());
            }
        }

        try (InputStream in = new ClassPathResource(PT_INTENT_MODEL_CLASSPATH).getInputStream()) {
            DoccatModel model = new DoccatModel(in);
            log.info("Modelo Doccat OpenNLP carregado via classpath: {}", PT_INTENT_MODEL_CLASSPATH);
            return new DocumentCategorizerME(model);
        } catch (Exception ex) {
            log.info("Modelo Doccat OpenNLP indisponivel no classpath ({}).", PT_INTENT_MODEL_CLASSPATH);
            return null;
        }
    }
}

