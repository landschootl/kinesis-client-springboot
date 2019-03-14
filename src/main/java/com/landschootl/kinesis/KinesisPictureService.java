package com.landschootl.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Service
@Slf4j
public class KinesisPictureService {
    //zone is an env variable
    @Value("${kinesis.region}")
    String region;

    @Value("${kinesis.stream}")
    String stream;

    @Value("${kinesis.accessKey}")
    String accessKey;

    @Value("${kinesis.secretKey}")
    String secretKey;

    private final ObjectMapper mapper;

    private AWSCredentialsProvider credentials;
    private AmazonKinesis client;

    private static final int BATCH_SIZE = 50;

    @Autowired
    public KinesisPictureService(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @PostConstruct
    public void init() {
        client();
    }

    private AmazonKinesis client() {
        if (client == null) {
            log.info("Creating Kinesis client.");
            final AmazonKinesisClientBuilder builder =
                    AmazonKinesisClientBuilder.standard()
                            .withRegion(region)
                            .withCredentials(credentials());
            client = builder.build();
        }
        return client;
    }


    private AWSCredentialsProvider credentials() {
        if (credentials == null) {
            credentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        }
        return credentials;
    }

    /**
     * Publishes data in multiple packages (batch) to avoid overloading kinesis
     * @param datas the data
     */
    public void publish(List<DataKinesis> datas) {
        log.debug("{} defects to re-send (and to report to InfluxDB).", datas.size());
        Collection<List<PutRecordsRequestEntry>> toResend = datas
                .stream()
                .map(this::toRecord)
                .collect(subDivider(BATCH_SIZE))
                .values();

        final int nbBatch = toResend.size();
        final AtomicInteger i = new AtomicInteger(0);
        toResend.forEach(batchSend -> {
            final PutRecordsRequest req = new PutRecordsRequest()
                    .withStreamName(stream)
                    .withRecords(batchSend);

            log.debug("Resending defect batch {} / {}", i.incrementAndGet(), nbBatch);
            final PutRecordsResult res = client().putRecords(req);
            log.debug("Defect batch {} sent. {} items failed within batch.", i.get(), res.getFailedRecordCount());

            publishFailRecord(nbBatch, i, batchSend, req, res);
        });
    }

    /**
     * Divides the data into multiple lists
     * @param batchSize the number of data per list
     * @param <T> the type of data
     * @return
     */
    private <T> Collector<T, ?, Map<Integer, List<T>>> subDivider(int batchSize) {
        final AtomicInteger i = new AtomicInteger(0);
        return Collectors.groupingBy(el -> i.getAndIncrement() / batchSize);
    }

    private void publishFailRecord(int nbBatch, AtomicInteger i, List<PutRecordsRequestEntry> batchSend, PutRecordsRequest req, PutRecordsResult res) {
        AtomicInteger nbFail = new AtomicInteger(0);
        while (res.getFailedRecordCount() > 0) {
            final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
            final List<PutRecordsResultEntry> putRecordsResultEntryList = res.getRecords();
            for (int j = 0; j < putRecordsResultEntryList.size(); j++) {
                final PutRecordsRequestEntry putRecordRequestEntry = batchSend.get(j);
                final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(j);
                if (putRecordsResultEntry.getErrorCode() != null) {
                    failedRecordsList.add(putRecordRequestEntry);
                }
            }
            batchSend = failedRecordsList;
            req.setRecords(batchSend);

            log.debug("Resending batch {} / {}", i, nbBatch);
            res = client.putRecords(req);
            log.debug("Defect batch {} resent. {} prices failed within batch.", i.get(), res.getFailedRecordCount());

            if(nbFail.getAndIncrement() == 1500){
                log.error("ERROR: Defect batch {} resent");
            }
        }
    }

    private byte[] toByte(DataKinesis dataKinesis) throws JsonProcessingException {
        return mapper.writeValueAsBytes(dataKinesis);
    }

    private ByteBuffer toBuffer(DataKinesis dataKinesis) throws JsonProcessingException {
        return ByteBuffer.wrap(toByte(dataKinesis));
    }

    private PutRecordsRequestEntry toRecord(DataKinesis dataKinesis) {
        try {
            return new PutRecordsRequestEntry().withData(toBuffer(dataKinesis))
                    .withPartitionKey(dataKinesis.getKey());
        } catch(Exception jpe) {
            log.error("Error to record kinesis", jpe);
            return null;
        }
    }
}
