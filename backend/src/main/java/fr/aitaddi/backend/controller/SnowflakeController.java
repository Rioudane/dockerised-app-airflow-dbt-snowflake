package fr.aitaddi.backend.controller;

import fr.aitaddi.backend.service.SnowflakeExportService;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.zip.ZipOutputStream;


@RestController
@RequestMapping("/snowflake")
public class SnowflakeController {

    private final SnowflakeExportService exportService;


    public SnowflakeController(SnowflakeExportService exportService) {
        this.exportService = exportService;
    }

    @GetMapping("/export_all")
    public void exportAllTables(HttpServletResponse response) throws Exception {
        response.setContentType("application/zip");
        response.setHeader("Content-Disposition", "attachment; filename=\"snowflake_tables.zip\"");

        try (ZipOutputStream zipOut = new ZipOutputStream(response.getOutputStream())) {
            exportService.exportAllTablesToZip(zipOut);
            zipOut.finish();
        }
    }
}

