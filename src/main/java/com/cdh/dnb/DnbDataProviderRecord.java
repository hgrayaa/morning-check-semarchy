package com.cdh.dnb;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Enregistrement DataProvider issu d'une notification D&B.
 *
 * Volontairement creux : ne porte QUE les champs effectivement mutes. Un champ
 * absent de la map signifie "non modifie" et ne doit pas etre ecrase en aval.
 * C'est cette distinction qui permet a la procedure SQL de reprendre la valeur
 * existante plutot que d'ecrire un null recu.
 *
 * Les cles sont les noms de champs du contrat REST Semarchy
 * (schema Persistable_DataProvider_), en camelCase.
 */
public class DnbDataProviderRecord {

    /** DUNS, ou subjectID selon le mode d'enregistrement du portefeuille. */
    private final String identifier;

    private final Map<String, String> mutatedFields = new LinkedHashMap<>();

    public DnbDataProviderRecord(String identifier) {
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }

    /**
     * Enregistre une mutation. Une valeur null est conservee telle quelle :
     * elle signifie "champ vide par D&B", ce qui differe de "champ absent".
     */
    public void put(String field, String value) {
        mutatedFields.put(field, value);
    }

    public boolean isEmpty() {
        return mutatedFields.isEmpty();
    }

    public boolean hasField(String field) {
        return mutatedFields.containsKey(field);
    }

    public String get(String field) {
        return mutatedFields.get(field);
    }

    public Map<String, String> getMutatedFields() {
        return mutatedFields;
    }

    @Override
    public String toString() {
        return "DnbDataProviderRecord{id=" + identifier + ", fields=" + mutatedFields + "}";
    }
}
