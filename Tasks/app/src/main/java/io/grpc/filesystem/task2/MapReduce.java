/*
 * the MapReduce functionality implemented in this program takes a single large text file to map i.e. split it into small chunks
 * Then, all words are assigned an initial count of one
 * Finally, it reduces by counting the unique words
 */

package io.grpc.filesystem.task2;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Map;

public class MapReduce {

    private static final int CHUNK_SIZE = 500;

    /**
     * Splits the input file into smaller chunks and stores them in a temporary directory.
     *
     * @param inputFilePath The path to the input file to be split.
     * @return The path of the directory where chunks are stored.
     * @throws IOException If an error occurs during file I/O.
     */
    public static String makeChunks(String inputFilePath) throws IOException {
        int count = 1;
        File inputFile = new File(inputFilePath);
        File chunkDir = new File(inputFile.getParent() + "/temp");
        if (!chunkDir.exists()) {
            chunkDir.mkdirs();
        }

        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String line = br.readLine();

            while (line != null) {
                File chunkFile = new File(chunkDir, "chunk" + String.format("%03d", count++) + ".txt");
                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(chunkFile))) {
                    int fileSize = 0;
                    while (line != null) {
                        byte[] bytes = (line + System.lineSeparator()).getBytes(Charset.defaultCharset());
                        if (bytes.length > CHUNK_SIZE) {
                            System.err.println("Skipping line exceeding chunk size: " + line);
                            line = br.readLine();
                            continue;
                        }
                        if (fileSize + bytes.length > CHUNK_SIZE)
                            break;
                        out.write(bytes);
                        fileSize += bytes.length;
                        line = br.readLine();
                    }
                }
            }
        }
        return chunkDir.getPath();
    }

    /**
     * Filters punctuations from the given line of text.
     *
     * @param line The line of text to be filtered.
     * @return The filtered line of text.
     */
    public static String filterPunctuations(String line) {
        if (line == null) return "";

        String filtered = line.replaceAll("[^a-zA-Z0-9\\s]", "");

        filtered = filtered.trim().replaceAll("\\s+", " ");
        return filtered.toLowerCase();
    }

    /**
     * Splits the given line of text into words.
     *
     * @param line The line of text to split.
     * @return An array of words from the input line.
     */
    public static String[] splitTextIntoWords(String line) {
        if (line == null) return new String[0];
        // Teilt die Zeile an Whitespaces
        return line.trim().split("\\s+");
    }

    /**
     * Checks if a given word is valid (alphanumeric only).
     *
     * @param word The word to check.
     * @return True if the word is valid, false otherwise.
     */
    public static boolean isValidWord(String word) {
        if (word == null || word.isEmpty()) return false;
        // Prüft, ob das Wort nur aus Buchstaben und Zahlen besteht
        return word.matches("^[a-zA-Z0-9]+$");
    }

    /**
     * Maps the content of each file chunk by filtering, splitting, and counting words.
     *
     * @param inputFilePath The path of the file chunk to process.
     * @throws IOException If an error occurs during file I/O.
     */
    public static void map(String inputFilePath) throws IOException {
        File inputFile = new File(inputFilePath);
        File mapFile = new File(inputFile.getParent(), "map-" + inputFile.getName().replace(".txt", "") + ".txt");
        //java.util.HashMap<String, Integer> wordCount = new java.util.HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile));
        BufferedWriter bw = new BufferedWriter(new FileWriter(mapFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String filtered = filterPunctuations(line);
                String[] words = splitTextIntoWords(filtered);
                for (String word : words) {
                    if (isValidWord(word)) {
                        word = word.toLowerCase();
                        bw.write(word + ":1");
                        bw.newLine();
                    }
                }
            }
        }


    }

    /**
     * Collects word-count pairs from map files.
     *
     * @param mapFiles An array of map file paths.
     * @return A map containing word counts.
     * @throws IOException If an error occurs during file I/O.
     */
    public static Map<String, Integer> collectWordCounts(String[] mapFiles) throws IOException {
        java.util.HashMap<String, Integer> totalCounts = new java.util.HashMap<>();
        for (String mapFilePath : mapFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(mapFilePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(":");
                    System.out.println("Reading line: " + line);
                    if (parts.length == 2) {
                        String word = parts[0];
                        int count = Integer.parseInt(parts[1]);
                        totalCounts.put(word, totalCounts.getOrDefault(word, 0) + count);
                    }
                }
            }
        }

        return totalCounts;

    }

    /**
     * Reduces the mapped word counts into a final result file.
     *
     * @param mapDirPath     The path of the directory containing map files.
     * @param outputFilePath The path of the final output file.
     * @throws IOException If an error occurs during file I/O.
     */
    public static void reduce(String mapDirPath, String outputFilePath) throws IOException {
        File mapDir = new File(mapDirPath);

        File[] mapFiles = mapDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("map") && name.endsWith(".txt");
            }
        });
        if (mapFiles == null) return;
        String[] mapFilePaths = new String[mapFiles.length];
        for (int i = 0; i < mapFiles.length; i++) {
            mapFilePaths[i] = mapFiles[i].getPath();
        }
        Map<String, Integer> wordCounts = collectWordCounts(mapFilePaths);
        storeFinalCounts(wordCounts, outputFilePath);
    }

    /**
     * Sorts the word counts and stores them in the final output file.
     *
     * @param wordCounts     The map of word counts to be sorted and stored.
     * @param outputFilePath The file to store the sorted word counts.
     * @throws IOException If an error occurs during file I/O.
     */
    public static void storeFinalCounts(Map<String, Integer> wordCounts, String outputFilePath) throws IOException {

        java.util.List<Map.Entry<String, Integer>> entries = new java.util.ArrayList<>(wordCounts.entrySet());


        java.util.Collections.sort(entries, new java.util.Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
                int cmp = e2.getValue().compareTo(e1.getValue());
                if (cmp == 0) {
                    return e1.getKey().compareTo(e2.getKey());
                }
                return cmp;
            }
        });

        // Ausgabe in die Datei schreiben
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (Map.Entry<String, Integer> entry : entries) {
                bw.write(entry.getKey() + ":" + entry.getValue());
                bw.newLine();
            }
        }
    }

    public static void main(String[] args) throws IOException { // update the main function if required
        if (args.length < 2) {
            System.out.println("Usage: <inputFilePath> <outputFilePath>");
            return;
        }
        String inputFilePath = args[0];
        String outputFilePath = args[1];

        // Split input file into chunks
        String chunkDirPath = makeChunks(inputFilePath);

        // Map phase: Process each chunk
        File chunkDir = new File(chunkDirPath);
        File[] chunkFiles = chunkDir.listFiles((dir, name) -> name.startsWith("chunk"));

        if (chunkFiles != null) {
            for (File chunkFile : chunkFiles) {
                map(chunkFile.getPath());
            }
        }

        // Reduce phase: Aggregate map results
        reduce(chunkDirPath, outputFilePath);
    }
}