package com.victor.sql_api.assistant.metadata_retrieval.application;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.InputStream;

@Component
public class OpenNlpPosTaggerProvider {
    private static final Logger log = LoggerFactory.getLogger(OpenNlpPosTaggerProvider.class);
    private static final String PT_POS_MODEL_CLASSPATH = "nlp/opennlp-pt-ud-gsd-pos-1.3-2.5.4.bin";

    private final POSTaggerME posTagger;

    public OpenNlpPosTaggerProvider(@Value("${app.nlp.pos-model-path:}") String posModelPath) {
        this.posTagger = loadPosTagger(posModelPath);
    }

    public POSTaggerME getPosTagger() {
        return posTagger;
    }

    private POSTaggerME loadPosTagger(String externalPath) {
        if (externalPath != null && !externalPath.isBlank()) {
            try (InputStream input = new FileInputStream(externalPath.trim())) {
                POSModel model = new POSModel(input);
                log.info("Modelo POS OpenNLP carregado via application.properties: {}", externalPath.trim());
                return new POSTaggerME(model);
            } catch (Exception ex) {
                log.warn("Falha ao carregar modelo POS OpenNLP via app.nlp.pos-model-path={}. Motivo: {}", externalPath.trim(), ex.getMessage());
            }
        }

        try (InputStream input = new ClassPathResource(PT_POS_MODEL_CLASSPATH).getInputStream()) {
            POSModel model = new POSModel(input);
            log.info("Modelo POS OpenNLP carregado via classpath: {}", PT_POS_MODEL_CLASSPATH);
            return new POSTaggerME(model);
        } catch (Exception ex) {
            log.info("Modelo POS OpenNLP indisponivel no classpath ({}).", PT_POS_MODEL_CLASSPATH);
            return null;
        }
    }
}
