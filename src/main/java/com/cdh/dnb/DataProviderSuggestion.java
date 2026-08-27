package com.cdh.dnb;

/**
 * Une ligne candidate a inserer dans SA_DATA_PROVIDER.
 *
 * Volontairement un simple porteur de valeurs : aucune logique metier ici.
 * Les champs correspondent exactement aux colonnes ecrites par
 * {@link DataProviderWriter}, dans le meme ordre.
 */
public class DataProviderSuggestion {

    private String providerCode;
    private String dataProviderNumber;
    private String legalName;
    private String commercialName;
    private String status;
    private String addressLine1;
    private String addressLine2;
    private String postalCode;
    private String city;
    private String countryCode;
    private String country;
    private String nationalId;
    private String nationalIdType;
    private Double score;
    private String message;
    private String fsAccount;
    private String fpAccount;

    public String getProviderCode() {
        return providerCode;
    }

    public DataProviderSuggestion setProviderCode(String providerCode) {
        this.providerCode = providerCode;
        return this;
    }

    public String getDataProviderNumber() {
        return dataProviderNumber;
    }

    public DataProviderSuggestion setDataProviderNumber(String dataProviderNumber) {
        this.dataProviderNumber = dataProviderNumber;
        return this;
    }

    public String getLegalName() {
        return legalName;
    }

    public DataProviderSuggestion setLegalName(String legalName) {
        this.legalName = legalName;
        return this;
    }

    public String getCommercialName() {
        return commercialName;
    }

    public DataProviderSuggestion setCommercialName(String commercialName) {
        this.commercialName = commercialName;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public DataProviderSuggestion setStatus(String status) {
        this.status = status;
        return this;
    }

    public String getAddressLine1() {
        return addressLine1;
    }

    public DataProviderSuggestion setAddressLine1(String addressLine1) {
        this.addressLine1 = addressLine1;
        return this;
    }

    public String getAddressLine2() {
        return addressLine2;
    }

    public DataProviderSuggestion setAddressLine2(String addressLine2) {
        this.addressLine2 = addressLine2;
        return this;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public DataProviderSuggestion setPostalCode(String postalCode) {
        this.postalCode = postalCode;
        return this;
    }

    public String getCity() {
        return city;
    }

    public DataProviderSuggestion setCity(String city) {
        this.city = city;
        return this;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public DataProviderSuggestion setCountryCode(String countryCode) {
        this.countryCode = countryCode;
        return this;
    }

    public String getCountry() {
        return country;
    }

    public DataProviderSuggestion setCountry(String country) {
        this.country = country;
        return this;
    }

    public String getNationalId() {
        return nationalId;
    }

    public DataProviderSuggestion setNationalId(String nationalId) {
        this.nationalId = nationalId;
        return this;
    }

    public String getNationalIdType() {
        return nationalIdType;
    }

    public DataProviderSuggestion setNationalIdType(String nationalIdType) {
        this.nationalIdType = nationalIdType;
        return this;
    }

    public Double getScore() {
        return score;
    }

    public DataProviderSuggestion setScore(Double score) {
        this.score = score;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public DataProviderSuggestion setMessage(String message) {
        this.message = message;
        return this;
    }

    public String getFsAccount() {
        return fsAccount;
    }

    public DataProviderSuggestion setFsAccount(String fsAccount) {
        this.fsAccount = fsAccount;
        return this;
    }

    public String getFpAccount() {
        return fpAccount;
    }

    public DataProviderSuggestion setFpAccount(String fpAccount) {
        this.fpAccount = fpAccount;
        return this;
    }

    @Override
    public String toString() {
        return "DataProviderSuggestion{number=" + dataProviderNumber
                + ", legalName=" + legalName
                + ", fsAccount=" + fsAccount
                + ", score=" + score + "}";
    }
}
