import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class UploadService {

    @Value("${gcs.bucketName}")
    private String bucketName; // Replace with your GCS bucket name

    private static final String BASE_PATH = "/path/to/your/folder"; // Replace this with the actual path to your folder

    public Mono<String> processFile(String reportType) {
        try {
            List<Path> files;
            if ("daily".equalsIgnoreCase(reportType)) {
                files = getFilesForDailyReport();
            } else if ("monthly".equalsIgnoreCase(reportType)) {
                files = getFilesForMonthlyReport();
            } else {
                return Mono.error(new IllegalArgumentException("Invalid report type"));
            }

            uploadFilesToGCS(files);
            return Mono.just("Files uploaded successfully");
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Error processing files", e));
        }
    }

    private List<Path> getFilesForDailyReport() throws Exception {
        LocalDate yesterday = LocalDate.now().minusDays(1);
        String yesterdayDate = yesterday.format(DateTimeFormatter.ofPattern("ddMMyy"));
        return Files.list(Paths.get(BASE_PATH))
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().startsWith(yesterdayDate))
                    .collect(Collectors.toList());
    }

    private List<Path> getFilesForMonthlyReport() throws Exception {
        YearMonth currentYearMonth = YearMonth.now();
        String currentMonth = currentYearMonth.format(DateTimeFormatter.ofPattern("MM"));
        return Files.list(Paths.get(BASE_PATH))
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().substring(2, 4).equals(currentMonth)) // Assuming file name format is ddMMyy
                    .collect(Collectors.toList());
    }

    private void uploadFilesToGCS(List<Path> files) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        for (Path file : files) {
            String objectName = file.getFileName().toString();
            BlobId blobId = BlobId.of(bucketName, objectName);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
            try (InputStream inputStream = Files.newInputStream(file)) {
                storage.create(blobInfo, inputStream);
                System.out.println("Uploaded file: " + objectName);
            } catch (Exception e) {
                // Handle GCS upload errors
                e.printStackTrace();
            }
        }
    }
}
 {

    @Value("${gcs.bucketName}")
    private String bucketName; // Replace with your GCS bucket name

    private static final String BASE_PATH = "/path/to/your/folder"; // Replace this with the actual path to your folder

    public Mono<String> processFile(String reportType) {
        try {
            List<Path> files;
            if ("daily".equalsIgnoreCase(reportType)) {
                files = getFilesForDailyReport();
            } else if ("monthly".equalsIgnoreCase(reportType)) {
                files = getFilesForMonthlyReport();
            } else {
                return Mono.error(new IllegalArgumentException("Invalid report type"));
            }

            uploadFilesToGCS(files);
            return Mono.just("Files uploaded successfully");
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Error processing files", e));
        }
    }

    private List<Path> getFilesForDailyReport() throws Exception {
        LocalDate yesterday = LocalDate.now().minusDays(1);
        String yesterdayDate = yesterday.format(DateTimeFormatter.ofPattern("ddMMyy"));
        return Files.list(Paths.get(BASE_PATH))
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().startsWith(yesterdayDate))
                    .collect(Collectors.toList());
    }

    private List<Path> getFilesForMonthlyReport() throws Exception {
        YearMonth currentYearMonth = YearMonth.now();
        String currentMonth = currentYearMonth.format(DateTimeFormatter.ofPattern("MM"));
        return Files.list(Paths.get(BASE_PATH))
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().substring(2, 4).equals(currentMonth)) // Assuming file name format is ddMMyy
                    .collect(Collectors.toList());
    }

    private void uploadFilesToGCS(List<Path> files) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        for (Path file : files) {
            String objectName = file.getFileName().toString();
            BlobId blobId = BlobId.of(bucketName, objectName);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
            try (InputStream inputStream = Files.newInputStream(file)) {
                storage.create(blobInfo, inputStream);
                System.out.println("Uploaded file: " + objectName);
            } catch (Exception e) {
                // Handle GCS upload errors
                e.printStackTrace();
            }
        }
    }
}
