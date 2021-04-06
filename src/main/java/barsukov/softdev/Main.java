package barsukov.softdev;

import barsukov.softdev.util.ProgressBar;
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
    private static final String SCAN = "scan";
    private static final String o    = "./transformedLog.json";
    private static final String e    = "./errors.json";
    private static final String k    = "./logsKeys.json";
    private static       String f    = null;

    private static long processedBytes = 0L;

    public static void main(String[] args) throws IOException {
        if (args.length == 2) {
            f = args[1];
        } else {
            System.out.println("Args is not set!");
            System.exit(1);
        }

        try (Stream<String> lines = Files.lines(Paths.get(f), Charset.defaultCharset()); RandomAccessFile oStream = new RandomAccessFile(o, "rw"); RandomAccessFile eStream = new RandomAccessFile(e, "rw"); FileChannel oChannel = oStream.getChannel(); FileChannel eChannel = eStream.getChannel();) {
            if (SCAN.equals(args[0])) {
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
        JSONObject jsonLog       = new JSONObject(line);
        JSONObject jsonLogTransf = new JSONObject();
        jsonLog.keySet().forEach(key -> jsonLogTransf.put(key, transformValue(key, jsonLog)));
        try {
            byte[]     strBytes = (jsonLogTransf.toString() + "\r\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer   = ByteBuffer.allocate(strBytes.length);
            buffer.put(strBytes);
            buffer.flip();
            oChannel.write(buffer);
            processedBytes += strBytes.length;
        } catch (IOException e) {
            try {
                byte[]     strBytes = (line + "\r\n").getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer   = ByteBuffer.allocate(strBytes.length);
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

    private static String transformValue(String key, JSONObject jsonLog) {
        return jsonLog.get(key).toString();
    }
}