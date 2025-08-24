package fr.aitaddi.backend.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.net.URL;
import java.time.Duration;


@RestController
@RequestMapping("/api")
public class UploadController {

    private final S3Presigner s3Presigner;

    @Value("${aws.bucket.name}")
    private String bucketName;

    public UploadController(S3Presigner s3Presigner) {
        this.s3Presigner = s3Presigner;
    }

    @PostMapping("/upload-url")
    public ResponseEntity<?> getPresignedUrl(@RequestParam String filename, @RequestParam String contentType) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key("raw_data/" + filename)
                .contentType(contentType)
                .build();

        PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(10))
                .putObjectRequest(objectRequest)
                .build();

        URL url = s3Presigner.presignPutObject(presignRequest).url();
        return ResponseEntity.ok().body(url.toString());
    }
}
