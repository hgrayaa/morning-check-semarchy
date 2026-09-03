package com.cdh.dnb;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Depot de suggestions DataProvider via l'API REST de persistance Semarchy,
 * dans le continuous load CL_LOADAPI_DNB.
 *
 * Variante API de DnbDataProviderFunction, qui ecrit en SQL direct. Les deux
 * coexistent le temps de comparer : meme jeu d'essai, deux voies de depot.
 *
 * Aucun appel a l'API D&B : les enregistrements sont fabriques en dur.
 */
public class DnbPersistApiFunction {

    private static final String DEFAULT_BASE_URL =
            "https://cdh-dev-wa-01.azurewebsites.net/api/rest";
    private static final String DEFAULT_DATA_LOCATION = "Account_dev";
    private static final String DEFAULT_LOAD_NAME = "CL_LOADAPI_DNB";
    private static final String DEFAULT_PROVIDER_CODE = "DNB";
    private static final String DEFAULT_CERTIFY_PROCESS = "false";
    private static final String DEFAULT_MESSAGE = "DNB_MONITORING";

    @FunctionName("DnbPersistApiTest")
    public HttpResponseMessage run(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.POST},
                    authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        var log = context.getLogger();
        String runId = context.getInvocationId();
        log.info("DNB_API_RUN_START runId=" + runId);

        try {
            String baseUrl = getenv("SEMARCHY_REST_URL", DEFAULT_BASE_URL);
            String dataLocation = getenv("SEMARCHY_DATA_LOCATION", DEFAULT_DATA_LOCATION);
            String loadName = getenv("DNB_CONTINUOUS_LOAD", DEFAULT_LOAD_NAME);
            String providerCode = getenv("DNB_PROVIDER_CODE", DEFAULT_PROVIDER_CODE);
            String certifyProcess = getenv("DNB_CERTIFY_PROCESS", DEFAULT_CERTIFY_PROCESS);
            String message = getenv("DNB_MESSAGE", DEFAULT_MESSAGE);

            String apiKey = System.getenv("SEMARCHY_API_KEY");
            String user = System.getenv("SEMARCHY_REST_USER");
            String password = System.getenv("SEMARCHY_REST_PASSWORD");

            if (isBlank(apiKey) && (isBlank(user) || isBlank(password))) {
                throw new IllegalStateException(
                        "Fournir SEMARCHY_API_KEY, ou SEMARCHY_REST_USER + SEMARCHY_REST_PASSWORD");
            }

            List<String> enrichers = parseEnrichers(getenv("DNB_ENRICHERS", ""));

            SemarchyPersistClient client = new SemarchyPersistClient(
                    baseUrl, dataLocation, loadName,
                    user, password, apiKey,
                    providerCode, certifyProcess,
                    enrichers, 30);

            List<DnbDataProviderRecord> records = buildSampleRecords();
            log.info("DNB_API_PAYLOAD runId=" + runId
                    + " load=" + loadName
                    + " records=" + records.size()
                    + " enrichers=" + enrichers);

            SemarchyPersistResult result = client.persist(records, message, log);

            log.info("DNB_API_RUN_END runId=" + runId + " status=OK " + result.trace());
            return request.createResponseBuilder(HttpStatus.OK)
                    .body("OK runId=" + runId + " " + result.trace())
                    .build();

        } catch (IllegalStateException e) {
            log.severe("DNB_API_CONFIG_KO runId=" + runId + " " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Configuration incomplete : " + e.getMessage())
                    .build();

        } catch (Exception e) {
            log.severe("DNB_API_RUN_END runId=" + runId
                    + " status=KO " + e.getClass().getName() + " - " + e.getMessage());
            Throwable c = e.getCause();
            while (c != null) {
                log.severe("Caused by: " + c.getClass().getName() + " - " + c.getMessage());
                c = c.getCause();
            }
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("KO runId=" + runId + " : " + e.getMessage())
                    .build();
        }
    }

    /**
     * Jeu d'essai. Chaque enregistrement ne porte que quelques champs, comme le
     * ferait une notification UPDATE reelle : un changement de nom, un
     * demenagement, un changement de statut.
     */
    private List<DnbDataProviderRecord> buildSampleRecords() {
        List<DnbDataProviderRecord> list = new ArrayList<>();

        DnbDataProviderRecord renamed = new DnbDataProviderRecord("804735132");
        renamed.put("legalName", "GORMAN MANUFACTURING COMPANY, INC.");
        renamed.put("commercialName", "GORMAN MANUFACTURING");
        list.add(renamed);

        DnbDataProviderRecord moved = new DnbDataProviderRecord("274845849");
        moved.put("addressLine1", "14 BOULEVARD GARIBALDI");
        moved.put("postalCode", "92130");
        moved.put("city", "ISSY-LES-MOULINEAUX");
        moved.put("countryCode", "FR");
        list.add(moved);

        DnbDataProviderRecord closed = new DnbDataProviderRecord("275454064");
        closed.put("status", "Inactive");
        list.add(closed);

        return list;
    }

    private static List<String> parseEnrichers(String raw) {
        if (isBlank(raw)) {
            return Collections.emptyList();
        }
        List<String> out = new ArrayList<>();
        for (String s : Arrays.asList(raw.split(","))) {
            if (!isBlank(s)) {
                out.add(s.trim());
            }
        }
        return out;
    }

    private static String getenv(String key, String def) {
        String v = System.getenv(key);
        return isBlank(v) ? def : v.trim();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
