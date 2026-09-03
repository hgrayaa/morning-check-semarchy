package com.cdh.dnb;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Resultat d'un appel PERSIST_DATA a l'API REST Semarchy.
 *
 * Reprend les champs du schema StatusRecordsAndLoad. Le detail par entite
 * (persistSummary) est ce qui repond au critere "rien ne disparait
 * silencieusement" : chaque enregistrement rejete est compte et identifiable.
 */
public class SemarchyPersistResult {

    /** PERSISTED ou PERSIST_CANCELLED. */
    private String status;

    private Integer loadId;
    private String loadStatus;
    private String continuousLoadName;

    private int recordsPersisted;
    private int recordsWithFailedValidations;
    private int recordsWithPotentialMatches;

    private int httpStatus;
    private String rawResponse;

    private final Map<String, String> rejections = new LinkedHashMap<>();

    public boolean isPersisted() {
        return "PERSISTED".equals(status);
    }

    public boolean hasRejections() {
        return recordsWithFailedValidations > 0 || recordsWithPotentialMatches > 0;
    }

    /** Trace compacte pour les logs, dans le format utilise par les enrichers. */
    public String trace() {
        return "HTTP_" + httpStatus
                + "_" + (status == null ? "NOSTATUS" : status)
                + "_LOAD_" + loadId
                + "_OK_" + recordsPersisted
                + "_KO_" + recordsWithFailedValidations
                + "_MATCH_" + recordsWithPotentialMatches;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getLoadId() {
        return loadId;
    }

    public void setLoadId(Integer loadId) {
        this.loadId = loadId;
    }

    public String getLoadStatus() {
        return loadStatus;
    }

    public void setLoadStatus(String loadStatus) {
        this.loadStatus = loadStatus;
    }

    public String getContinuousLoadName() {
        return continuousLoadName;
    }

    public void setContinuousLoadName(String continuousLoadName) {
        this.continuousLoadName = continuousLoadName;
    }

    public int getRecordsPersisted() {
        return recordsPersisted;
    }

    public void setRecordsPersisted(int recordsPersisted) {
        this.recordsPersisted = recordsPersisted;
    }

    public int getRecordsWithFailedValidations() {
        return recordsWithFailedValidations;
    }

    public void setRecordsWithFailedValidations(int recordsWithFailedValidations) {
        this.recordsWithFailedValidations = recordsWithFailedValidations;
    }

    public int getRecordsWithPotentialMatches() {
        return recordsWithPotentialMatches;
    }

    public void setRecordsWithPotentialMatches(int recordsWithPotentialMatches) {
        this.recordsWithPotentialMatches = recordsWithPotentialMatches;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public String getRawResponse() {
        return rawResponse;
    }

    public void setRawResponse(String rawResponse) {
        this.rawResponse = rawResponse;
    }

    public Map<String, String> getRejections() {
        return rejections;
    }

    @Override
    public String toString() {
        return trace();
    }
}
