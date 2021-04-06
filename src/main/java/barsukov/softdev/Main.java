package barsukov.softdev;

import barsukov.softdev.util.ProgressBar;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.LogOriginal;
import model.LogTransformed;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class Main {
    public static final String SCAN = "scan";
    static private ObjectMapper mapper = new ObjectMapper()
             .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
//    static private String f = "./phil.json";
    static private String f = null;
    static private String o = "./transformedLog.json";
    static private String e = "./errors.json";
    static private String k = "./logsKeys.json";

    static long processedBytes = 0L;

    public static void main(String[] args) throws IOException {
        if (args.length == 2){
            f = args[1];
        } else {
            System.out.println("Args is not set!");
            System.exit(1);
        }

        try (
            Stream<String> lines = Files.lines(Paths.get(f), Charset.defaultCharset());
            RandomAccessFile oStream = new RandomAccessFile(o, "rw");
            RandomAccessFile eStream = new RandomAccessFile(e, "rw");
            FileChannel oChannel = oStream.getChannel();
            FileChannel eChannel = eStream.getChannel();
        ) {
            if(SCAN.equals(args[0])) {
                Set<String> commonKeySet = new HashSet<>();
                lines.forEachOrdered(line -> scanLogKeys(line, commonKeySet));
                writeKeysToFile(commonKeySet);
            } else {
                lines.forEachOrdered(line -> processLog(line, oChannel, eChannel, new File(f).length()));
            }
        }
    }

    private static void writeKeysToFile(Set<String> commonKeySet) {
        File file = new File(k);
        try (BufferedWriter bf = new BufferedWriter(new FileWriter(file))) {
            for (String key : commonKeySet) {
                bf.write(key);
                bf.newLine();
            }
            bf.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void scanLogKeys(String line, Set<String> commonKeySet) {
        JSONObject jsonLog = new JSONObject(line);
        commonKeySet.addAll(jsonLog.keySet());
        processedBytes += line.getBytes().length;
        ProgressBar.updateProgressBar(processedBytes, new File(f).length());
    }

    private static void processLog(String line, FileChannel oChannel, FileChannel eChannel, long totalSize) {
        LogTransformed logtransformed = new LogTransformed();
        try {
            LogOriginal log = mapper.readValue(line, LogOriginal.class);
            map(log, logtransformed);
            byte[] strBytes = (mapper.writeValueAsString(logtransformed) + "\r\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(strBytes.length);
            buffer.put(strBytes);
            buffer.flip();
            oChannel.write(buffer);
            processedBytes += strBytes.length;
        } catch (IOException e) {
            try {
                byte[] strBytes = (line + "\r\n").getBytes(StandardCharsets.UTF_8);
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