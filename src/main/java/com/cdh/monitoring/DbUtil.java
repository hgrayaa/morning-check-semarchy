package com.cdh.monitoring;

import java.sql.*;
import java.util.*;

public final class DbUtil {

    private DbUtil() {}

    public static List<Map<String, Object>> query(String jdbcUrl, String user, String pass, String sql) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, pass);
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            List<Map<String, Object>> rows = new ArrayList<>();
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= cols; i++) {
                    row.put(md.getColumnLabel(i), rs.getObject(i));
                }
                rows.add(row);
            }
            return rows;
        }
    }

    public static String toHtmlTable(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) return "<p>(vide)</p>";

        StringBuilder sb = new StringBuilder();
        sb.append("<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;font-family:Arial;font-size:13px;'>");

        sb.append("<tr style='background:#f2f2f2;'>");
        for (String col : rows.get(0).keySet()) sb.append("<th>").append(escape(col)).append("</th>");
        sb.append("</tr>");

        for (Map<String, Object> r : rows) {
            sb.append("<tr>");
            for (Object v : r.values()) {
                sb.append("<td>").append(escape(v == null ? "" : String.valueOf(v))).append("</td>");
            }
            sb.append("</tr>");
        }
        sb.append("</table>");
        return sb.toString();
    }

    private static String escape(String s) {
        return s == null ? "" : s
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }
}
