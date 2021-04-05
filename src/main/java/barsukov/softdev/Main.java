package barsukov.softdev;

import barsukov.softdev.util.ProgressBar;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.LogOriginal;
import model.LogTransformed;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Main {
    static ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    static String f = null;
    static String o = "./transformedLog.json";
    static String e = "./errors.json";

    static long processedBytes = 0L;

    public static void main(String[] args) throws IOException {
        if (args.length != 0){
            f = args[0];
        } else {
            System.out.println("File is not set!");
            System.exit(1);
        }

        ProgressBar progressBar = new ProgressBar();
        try {
            Stream<String> lines = Files.lines(Paths.get(f), Charset.defaultCharset());
            RandomAccessFile oStream = new RandomAccessFile(o, "rw");
            RandomAccessFile eStream = new RandomAccessFile(e, "rw");
            FileChannel oChannel = oStream.getChannel();
            FileChannel eChannel = eStream.getChannel();

            lines.forEachOrdered(line -> transformLog(line, oChannel, eChannel, new File(f).length()));

        } catch (Exception e) {

        }
    }

    private static void transformLog(String line, FileChannel oChannel, FileChannel eChannel, long totalSize) {
        LogTransformed logtransformed = new LogTransformed();
        try {
            LogOriginal log = mapper.readValue(line, LogOriginal.class);
            map(log, logtransformed);
            byte[] strBytes = mapper.writeValueAsBytes(logtransformed);
            ByteBuffer buffer = ByteBuffer.allocate(strBytes.length);
            buffer.put(strBytes);
            buffer.flip();
            oChannel.write(buffer);
            processedBytes += strBytes.length;
        } catch (IOException e) {
            try {
                byte[] strBytes = line.getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer = ByteBuffer.allocate(strBytes.length);
                buffer.put(strBytes);
                buffer.flip();
                eChannel.write(buffer);
                processedBytes += strBytes.length;
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }

        ProgressBar.updateProgressBar(processedBytes, totalSize);

    }

    private static void map(LogOriginal log, LogTransformed transformedLog) {
        transformedLog.setUsername(log.getUsername());
        transformedLog.setEvent_type(log.getEvent_type());
        transformedLog.setIp(log.getIp());
        transformedLog.setAgent(log.getAgent());
        transformedLog.setHost(log.getHost());
        transformedLog.setReferer(log.getReferer());
        transformedLog.setAccept_language(log.getAccept_language());
        transformedLog.setEvent(log.getEvent());
        transformedLog.setEvent_source(log.getEvent_source());
        transformedLog.setTime(log.getTime());
        transformedLog.setPage(log.getPage());

        try {
            transformedLog.setContext(mapper.writeValueAsString(log.getContext()));
        } catch (JsonProcessingException jsonProcessingException) {
            jsonProcessingException.printStackTrace();
        }
    }
}