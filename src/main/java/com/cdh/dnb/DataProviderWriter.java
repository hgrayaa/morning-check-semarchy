package com.cdh.dnb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.logging.Logger;

/**
 * Ecriture des suggestions dans SA_DATA_PROVIDER.
 *
 * Portee volontairement restreinte : cette classe ne fait qu'inserer.
 * Aucune regle metier, aucun mapping D&B ici.
 *
 * Choix techniques :
 *  - data_provider_id est alimente par nextval('seq_data_provider') cote SQL.
 *    Fonctionne que la sequence soit ou non attachee en defaut de colonne.
 *  - b_loadid est passe en parametre (0 pour ce premier jalon, un vrai id de
 *    continuous load par la suite) : rien n'est code en dur.
 *  - stringtype=unspecified est ajoute a l'URL JDBC pour laisser PostgreSQL
 *    inferer le type des parametres textuels. Evite d'avoir a savoir si
 *    to_be_updated et certify_process sont declares en boolean ou en varchar.
 *  - Insertion en batch, une seule transaction, rollback global.
 *
 * Note : DbUtil (com.cdh.monitoring) n'expose que query(), en lecture seule et
 * sans transaction. L'ecriture justifie donc une classe dediee plutot qu'un
 * ajout dans DbUtil, afin de ne pas modifier une classe utilisee par les
 * fonctions de supervision existantes.
 */
public class DataProviderWriter {

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final String author;
    private final String schema;

    public DataProviderWriter(String jdbcUrl, String user, String password,
                              String author, String schema) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.author = author;
        this.schema = schema;
    }

    private String insertSql() {
        return "INSERT INTO " + schema + ".sa_data_provider ("
                + " data_provider_id, b_loadid, b_classname,"
                + " b_credate, b_creator, b_updator,"
                + " provider_code, data_provider_number,"
                + " legal_name, commercial_name, status,"
                + " address_line1, address_line2, postal_code, city,"
                + " country_code, country,"
                + " national_id, national_id_type,"
                + " score, message,"
                + " to_be_updated, certify_process,"
                + " fs_account, fp_account"
                + ") VALUES ("
                + " nextval('" + schema + ".seq_data_provider'), ?, 'DataProvider',"
                + " ?, ?, ?,"
                + " ?, ?,"
                + " ?, ?, ?,"
                + " ?, ?, ?, ?,"
                + " ?, ?,"
                + " ?, ?,"
                + " ?, ?,"
                + " ?, ?,"
                + " ?, ?"
                + ")";
    }

    /**
     * Insere les suggestions fournies avec le loadId indique.
     *
     * @return le nombre de lignes effectivement inserees
     * @throws SQLException si la transaction echoue (rien n'est alors ecrit)
     */
    public int insert(List<DataProviderSuggestion> suggestions, long loadId, Logger log)
            throws SQLException {

        if (suggestions == null || suggestions.isEmpty()) {
            log.warning("DNB_WRITE_SKIP aucune suggestion a inserer");
            return 0;
        }

        String url = jdbcUrl.contains("stringtype=")
                ? jdbcUrl
                : jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?") + "stringtype=unspecified";

        Timestamp now = Timestamp.from(Instant.now());

        try (Connection cnx = DriverManager.getConnection(url, user, password)) {
            cnx.setAutoCommit(false);

            try (PreparedStatement ps = cnx.prepareStatement(insertSql())) {
                for (DataProviderSuggestion s : suggestions) {
                    int i = 1;
                    ps.setLong(i++, loadId);
                    ps.setTimestamp(i++, now);
                    ps.setString(i++, author);
                    ps.setString(i++, author);

                    ps.setString(i++, s.getProviderCode());
                    ps.setString(i++, s.getDataProviderNumber());

                    ps.setString(i++, s.getLegalName());
                    ps.setString(i++, s.getCommercialName());
                    ps.setString(i++, s.getStatus());

                    ps.setString(i++, s.getAddressLine1());
                    ps.setString(i++, s.getAddressLine2());
                    ps.setString(i++, s.getPostalCode());
                    ps.setString(i++, s.getCity());

                    ps.setString(i++, s.getCountryCode());
                    ps.setString(i++, s.getCountry());

                    ps.setString(i++, s.getNationalId());
                    ps.setString(i++, s.getNationalIdType());

                    if (s.getScore() == null) {
                        ps.setNull(i++, Types.NUMERIC);
                    } else {
                        ps.setDouble(i++, s.getScore());
                    }
                    ps.setString(i++, s.getMessage());

                    ps.setString(i++, "true");   // to_be_updated
                    ps.setString(i++, "false");  // certify_process

                    ps.setString(i++, s.getFsAccount());
                    ps.setString(i, s.getFpAccount());

                    ps.addBatch();
                }

                int[] counts = ps.executeBatch();
                cnx.commit();

                int total = 0;
                for (int c : counts) {
                    if (c > 0) {
                        total += c;
                    }
                }
                log.info("DNB_WRITE_OK loadId=" + loadId + " rows=" + total);
                return total;

            } catch (SQLException e) {
                cnx.rollback();
                log.severe("DNB_WRITE_KO rollback loadId=" + loadId
                        + " sqlState=" + e.getSQLState() + " msg=" + e.getMessage());
                throw e;
            }
        }
    }
}
