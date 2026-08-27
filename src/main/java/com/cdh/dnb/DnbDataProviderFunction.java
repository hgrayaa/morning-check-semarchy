package com.cdh.dnb;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

/**
 * Premier jalon du flux D&B : on valide uniquement la partie aval du pipeline
 * (j'ai de la donnee -> je la depose dans SA_DATA_PROVIDER).
 *
 * Declencheur HTTP volontairement, et non minuteur : on controle exactement
 * quand l'insertion a lieu.
 *
 * Aucun appel a l'API D&B ici. Les suggestions sont fabriquees en dur, calquees
 * sur des lignes reelles de SA_DATA_PROVIDER, y compris le cas de deux
 * suggestions concurrentes pour la meme entreprise.
 *
 * Reutilise les app settings DB_* deja en place pour MorningCheckSemarchy :
 * aucune nouvelle variable de connexion n'est necessaire.
 */
public class DnbDataProviderFunction {

    private static final String DEFAULT_SCHEMA = "semarchy_data_location_account_dev";
    private static final String DEFAULT_AUTHOR = "DNB_FUNCTION";
    private static final String DEFAULT_LOAD_ID = "0";
    private static final String DEFAULT_PROVIDER_CODE = "NEXTBI";

    @FunctionName("DnbDataProviderTest")
    public HttpResponseMessage run(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.POST},
                    authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        var log = context.getLogger();
        String runId = context.getInvocationId();
        log.info("DNB_RUN_START runId=" + runId);

        try {
            // Memes app settings que MorningCheckFunction
            String dbHost = mustGet("DB_HOST");
            String dbPort = getenv("DB_PORT", "5432");
            String dbName = mustGet("DB_NAME");
            String dbUser = mustGet("DB_USER");
            String dbPass = mustGet("DB_PASS");

            String jdbcUrl = "jdbc:postgresql://"
                    + dbHost + ":" + dbPort + "/" + dbName
                    + "?sslmode=require";

            long loadId = Long.parseLong(getenv("DNB_LOAD_ID", DEFAULT_LOAD_ID));
            String author = getenv("DNB_AUTHOR", DEFAULT_AUTHOR);
            String providerCode = getenv("DNB_PROVIDER_CODE", DEFAULT_PROVIDER_CODE);
            String schema = getenv("DNB_DB_SCHEMA", DEFAULT_SCHEMA);

            List<DataProviderSuggestion> suggestions = buildSampleSuggestions(providerCode);
            log.info("DNB_PAYLOAD runId=" + runId
                    + " schema=" + schema
                    + " loadId=" + loadId
                    + " size=" + suggestions.size());

            DataProviderWriter writer =
                    new DataProviderWriter(jdbcUrl, dbUser, dbPass, author, schema);

            int inserted = writer.insert(suggestions, loadId, log);

            log.info("DNB_RUN_END runId=" + runId + " status=OK rows=" + inserted);
            return request.createResponseBuilder(HttpStatus.OK)
                    .body("OK runId=" + runId + " loadId=" + loadId + " rows=" + inserted)
                    .build();

        } catch (IllegalStateException e) {
            log.severe("DNB_CONFIG_KO runId=" + runId + " " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Configuration incomplete : " + e.getMessage())
                    .build();

        } catch (Exception e) {
            log.severe("DNB_RUN_END runId=" + runId
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
     * Jeu d'essai calque sur des lignes reelles de SA_DATA_PROVIDER.
     *
     * Les deux dernieres entrees portent volontairement le meme
     * data_provider_number et le meme national_id, mais des fs_account et des
     * scores differents : c'est le cas nominal de deux suggestions concurrentes
     * pour une meme entreprise.
     *
     * IMPORTANT : les fs_account / fp_account doivent exister reellement en DEV.
     * Les valeurs ci-dessous sont a remplacer par des couples releves en base.
     */
    private List<DataProviderSuggestion> buildSampleSuggestions(String providerCode) {
        List<DataProviderSuggestion> list = new ArrayList<>();

        list.add(new DataProviderSuggestion()
                .setProviderCode(providerCode)
                .setDataProviderNumber("FR000015132789")
                .setLegalName("CHARLES THIERRY")
                .setCommercialName("CHARLES THIERRY")
                .setStatus("active")
                .setAddressLine1("CENTRE COMMERCIAL")
                .setAddressLine2("2 RUE DU COMMERCE")
                .setPostalCode("81100")
                .setCity("CASTRES")
                .setCountryCode("FR")
                .setCountry("FRANCE")
                .setNationalId("39828468700017")
                .setNationalIdType("SIRET")
                .setScore(0.80d)
                .setMessage("10-active match")
                .setFsAccount("5d562516-7b75-11f1-8fd6-6a3e7bf3d323")
                .setFpAccount("IHM"));

        list.add(new DataProviderSuggestion()
                .setProviderCode(providerCode)
                .setDataProviderNumber("FR000038768071")
                .setLegalName("SEQENS SOCIETE ANONYME D'HABITATIONS A LOYER MODERE")
                .setCommercialName("SEQENS SOCIETE ANONYME D'HABITATIONS A LOYER MODERE")
                .setStatus("active")
                .setAddressLine1("14-16")
                .setAddressLine2("14 BOULEVARD GARIBALDI")
                .setPostalCode("92130")
                .setCity("ISSY-LES-MOULINEAUX")
                .setCountryCode("FR")
                .setCountry("FRANCE")
                .setNationalId("58214281600310")
                .setNationalIdType("SIRET")
                .setScore(0.29d)
                .setMessage("10-active match")
                .setFsAccount("ACCOUNT.001G500000rp3qaIAA")
                .setFpAccount("EFORCE"));

        list.add(new DataProviderSuggestion()
                .setProviderCode(providerCode)
                .setDataProviderNumber("FR000038768071")
                .setLegalName("SEQENS SOCIETE ANONYME D'HABITATIONS A LOYER MODERE")
                .setCommercialName("SEQENS SOCIETE ANONYME D'HABITATIONS A LOYER MODERE")
                .setStatus("active")
                .setAddressLine1("14-16")
                .setAddressLine2("14 BOULEVARD GARIBALDI")
                .setPostalCode("92130")
                .setCity("ISSY-LES-MOULINEAUX")
                .setCountryCode("FR")
                .setCountry("FRANCE")
                .setNationalId("58214281600310")
                .setNationalIdType("SIRET")
                .setScore(0.21d)
                .setMessage("10-active match")
                .setFsAccount("ACCOUNT.001G500000rouxYIAQ")
                .setFpAccount("EFORCE"));

        return list;
    }

    private static String mustGet(String key) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing app setting: " + key);
        }
        return v.trim();
    }

    private static String getenv(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }
}
