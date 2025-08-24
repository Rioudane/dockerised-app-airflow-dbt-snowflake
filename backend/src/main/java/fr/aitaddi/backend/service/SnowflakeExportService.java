package fr.aitaddi.backend.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
public class SnowflakeExportService {

    @Value("${snowflake.url}")
    private String url;

    @Value("${snowflake.username}")
    private String user;

    @Value("${snowflake.password}")
    private String password;

    @Value("${snowflake.schema}")
    private String schema;


    public void exportAllTablesToZip(ZipOutputStream zipOut) throws Exception {
        String tablesQuery = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "'";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(tablesQuery)) {

            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }

            for (String table : tables) {
                ZipEntry zipEntry = new ZipEntry(table + ".csv");
                zipOut.putNextEntry(zipEntry);

                exportSingleTable(conn, table, zipOut);

                zipOut.closeEntry();
            }
        }
    }

//    public void exportAllTablesToCSV(Path outputDir) throws Exception {
//        String tablesQuery = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "'";
//
//        try (Connection conn = DriverManager.getConnection(url, user, password);
//             Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery(tablesQuery)) {
//
//            List<String> tables = new ArrayList<>();
//            while (rs.next()) {
//                tables.add(rs.getString("TABLE_NAME"));
//            }
//
//            for (String table : tables) {
//                exportSingleTable(conn, table, outputDir.resolve(table + ".csv"));
//            }
//        }
//    }

    private void exportSingleTable(Connection conn, String table, ZipOutputStream zipOut) throws Exception {
        String query = "SELECT * FROM " + table;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // ⚠️ Do not use try-with-resources here, otherwise it will close zipOut!
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(zipOut));
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
                    IntStream.range(1, columnCount + 1)
                            .mapToObj(i -> {
                                try {
                                    return metaData.getColumnName(i);
                                } catch (SQLException e) {
                                    return "col" + i;
                                }
                            }).toArray(String[]::new)));

            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getString(i));
                }
                csvPrinter.printRecord(row);
            }

            csvPrinter.flush();  // flush, but DO NOT close
            writer.flush();      // flush, but DO NOT close
        }
    }
}
