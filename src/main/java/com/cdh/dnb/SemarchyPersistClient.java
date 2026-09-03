package com.cdh.dnb;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Depot des suggestions DataProvider via l'API REST de persistance Semarchy.
 *
 *   POST /loads/{dataLocation}/{continuousLoadName}
 *   { "action": "PERSIST_DATA", "persistOptions": {...}, "persistRecords": {...} }
 *
 * Le continuous load est adresse par son NOM, pas par un identifiant numerique
 * (parametre load-id-or-load-name de la spec). Le nom est stable d'un
 * environnement a l'autre : rien a recalculer, rien a persister.
 *
 * Options retenues :
 *  - missingIdBehavior GENERATE : Semarchy genere dataProviderID. Pas d'appel
 *    a seq_data_provider, contrairement au depot en SQL direct.
 *  - persistMode IF_NO_ERROR_OR_MATCH : un enregistrement en erreur n'entre
 *    pas dans le MDM, il est rejete et compte.
 *  - responsePayload SUMMARY_AND_RECORDS : la reponse porte le detail par
 *    entite, ce qui alimente la supervision.
 *  - enrichers : aucun par defaut. NormalizeSourceDataProvider est desactive
 *    dans le modele. CountryLookup reste activable par configuration.
 *
 * Les champs de rattachement au compte (FID_Account, PublisherID_Account,
 * SourceID_Account) ne sont volontairement pas alimentes : la jointure entre
 * dataProviderNumber et md_account est faite par la procedure en aval.
 */
public class SemarchyPersistClient {

    private static final Gson GSON = new Gson();

    private final String baseUrl;
    private final String dataLocation;
    private final String continuousLoadName;
    private final String user;
    private final String password;
    private final String apiKey;
    private final String providerCode;
    private final String certifyProcess;
    private final List<String> enrichers;
    private final HttpClient http;

    public SemarchyPersistClient(String baseUrl,
                                 String dataLocation,
                                 String continuousLoadName,
                                 String user,
                                 String password,
                                 String apiKey,
                                 String providerCode,
                                 String certifyProcess,
                                 List<String> enrichers,
                                 int timeoutSeconds) {
        this.baseUrl = trimTrailingSlash(baseUrl);
        this.dataLocation = dataLocation;
        this.continuousLoadName = continuousLoadName;
        this.user = user;
        this.password = password;
        this.apiKey = apiKey;
        this.providerCode = providerCode;
        this.certifyProcess = certifyProcess;
        this.enrichers = enrichers;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(timeoutSeconds))
                .build();
    }

    /**
     * Depose les enregistrements dans le continuous load.
     *
     * @param message valeur du champ technique message (trace d'origine)
     */
    public SemarchyPersistResult persist(List<DnbDataProviderRecord> records,
                                         String message,
                                         Logger log) throws Exception {

        SemarchyPersistResult result = new SemarchyPersistResult();
        result.setContinuousLoadName(continuousLoadName);

        if (records == null || records.isEmpty()) {
            log.warning("DNB_PERSIST_SKIP aucun enregistrement");
            result.setStatus("PERSISTED");
            return result;
        }

        String url = baseUrl + "/loads/" + dataLocation + "/" + continuousLoadName;
        String body = GSON.toJson(buildPayload(records, message));

        log.info("DNB_PERSIST_CALL url=" + url + " records=" + records.size());

        HttpRequest.Builder rb = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMinutes(5))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));

        applyAuth(rb);

        HttpResponse<String> response =
                http.send(rb.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        result.setHttpStatus(response.statusCode());
        result.setRawResponse(response.body());

        if (response.statusCode() != 200 && response.statusCode() != 202) {
            log.severe("DNB_PERSIST_KO http=" + response.statusCode()
                    + " body=" + truncate(response.body(), 800));
            throw new IllegalStateException(
                    "Persist Semarchy en echec, HTTP " + response.statusCode());
        }

        parseResponse(response.body(), result, log);
        log.info("DNB_PERSIST_OK " + result.trace());
        return result;
    }

    private JsonObject buildPayload(List<DnbDataProviderRecord> records, String message) {
        JsonObject entityOptions = new JsonObject();
        if (enrichers != null) {
            JsonArray arr = new JsonArray();
            for (String e : enrichers) {
                arr.add(e);
            }
            entityOptions.add("enrichers", arr);
        }

        JsonObject optionsPerEntity = new JsonObject();
        optionsPerEntity.add("DataProvider", entityOptions);

        JsonObject persistOptions = new JsonObject();
        persistOptions.addProperty("missingIdBehavior", "GENERATE");
        persistOptions.addProperty("persistMode", "IF_NO_ERROR_OR_MATCH");
        persistOptions.addProperty("responsePayload", "SUMMARY_AND_RECORDS");
        persistOptions.add("optionsPerEntity", optionsPerEntity);

        JsonArray dataProviders = new JsonArray();
        for (DnbDataProviderRecord r : records) {
            dataProviders.add(toJson(r, message));
        }

        JsonObject persistRecords = new JsonObject();
        persistRecords.add("DataProvider", dataProviders);

        JsonObject payload = new JsonObject();
        payload.addProperty("action", "PERSIST_DATA");
        payload.add("persistOptions", persistOptions);
        payload.add("persistRecords", persistRecords);
        return payload;
    }

    /**
     * Un enregistrement ne porte que les champs mutes, plus les champs fixes.
     * score reste absent : il qualifie un matching, or le DUNS est deja
     * rattache. toBeUpdated est un boolean, certifyProcess une chaine : le
     * typage suit le schema Persistable_DataProvider_.
     */
    private JsonObject toJson(DnbDataProviderRecord record, String message) {
        JsonObject o = new JsonObject();
        o.addProperty("providerCode", providerCode);
        o.addProperty("dataProviderNumber", record.getIdentifier());
        o.addProperty("toBeUpdated", true);

        if (certifyProcess != null) {
            o.addProperty("certifyProcess", certifyProcess);
        }
        if (message != null) {
            o.addProperty("message", message);
        }

        for (Map.Entry<String, String> e : record.getMutatedFields().entrySet()) {
            if (e.getValue() == null) {
                o.add(e.getKey(), null);
            } else {
                o.addProperty(e.getKey(), e.getValue());
            }
        }
        return o;
    }

    private void parseResponse(String body, SemarchyPersistResult result, Logger log) {
        if (body == null || body.isBlank()) {
            return;
        }
        JsonElement root = JsonParser.parseString(body);
        if (!root.isJsonObject()) {
            return;
        }
        JsonObject o = root.getAsJsonObject();

        if (o.has("status") && !o.get("status").isJsonNull()) {
            result.setStatus(o.get("status").getAsString());
        }

        if (o.has("load") && o.get("load").isJsonObject()) {
            JsonObject load = o.getAsJsonObject("load");
            if (load.has("loadId") && !load.get("loadId").isJsonNull()) {
                result.setLoadId(load.get("loadId").getAsInt());
            }
            if (load.has("loadStatus") && !load.get("loadStatus").isJsonNull()) {
                result.setLoadStatus(load.get("loadStatus").getAsString());
            }
        }

        if (o.has("persistSummary") && o.get("persistSummary").isJsonObject()) {
            JsonObject summary = o.getAsJsonObject("persistSummary");
            if (summary.has("DataProvider") && summary.get("DataProvider").isJsonObject()) {
                JsonObject dp = summary.getAsJsonObject("DataProvider");
                result.setRecordsPersisted(asInt(dp, "recordsPersisted"));
                result.setRecordsWithFailedValidations(asInt(dp, "recordsWithFailedValidations"));
                result.setRecordsWithPotentialMatches(asInt(dp, "recordsWithPotentialMatches"));
            }
        }

        if (result.hasRejections()) {
            log.warning("DNB_PERSIST_REJECTS " + result.trace());
            logRejectedRecords(o, result, log);
        }
    }

    /**
     * Trace chaque enregistrement rejete avec son identifiant et la cause.
     * Sans cela, le compteur global ne suffit pas a savoir ce qui a ete perdu.
     */
    private void logRejectedRecords(JsonObject root, SemarchyPersistResult result, Logger log) {
        if (!root.has("records") || !root.get("records").isJsonObject()) {
            return;
        }
        JsonObject records = root.getAsJsonObject("records");
        if (!records.has("DataProvider") || !records.get("DataProvider").isJsonArray()) {
            return;
        }
        for (JsonElement el : records.getAsJsonArray("DataProvider")) {
            if (!el.isJsonObject()) {
                continue;
            }
            JsonObject rec = el.getAsJsonObject();
            String number = rec.has("dataProviderNumber") && !rec.get("dataProviderNumber").isJsonNull()
                    ? rec.get("dataProviderNumber").getAsString()
                    : "?";

            if (rec.has("failedValidations") && rec.get("failedValidations").isJsonArray()
                    && rec.getAsJsonArray("failedValidations").size() > 0) {
                String cause = rec.get("failedValidations").toString();
                result.getRejections().put(number, cause);
                log.warning("DNB_PERSIST_REJECT duns=" + number
                        + " cause=" + truncate(cause, 400));
            }

            if (rec.has("potentialMatches") && rec.get("potentialMatches").isJsonArray()
                    && rec.getAsJsonArray("potentialMatches").size() > 0) {
                result.getRejections().put(number, "POTENTIAL_MATCH");
                log.warning("DNB_PERSIST_MATCH duns=" + number);
            }
        }
    }

    private void applyAuth(HttpRequest.Builder rb) {
        if (apiKey != null && !apiKey.isBlank()) {
            rb.header("Api-Key", apiKey);
            return;
        }
        String creds = user + ":" + password;
        String encoded = Base64.getEncoder()
                .encodeToString(creds.getBytes(StandardCharsets.UTF_8));
        rb.header("Authorization", "Basic " + encoded);
    }

    private static int asInt(JsonObject o, String key) {
        return o.has(key) && !o.get(key).isJsonNull() ? o.get(key).getAsInt() : 0;
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return null;
        }
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    private static String trimTrailingSlash(String s) {
        return s != null && s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }
}
