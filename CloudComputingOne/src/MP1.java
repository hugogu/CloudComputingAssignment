
import java.io.BufferedReader;
import java.io.FileReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.Map.Entry;

public class MP1 {

    Random generator;
    String userName;
    String inputFileName;
    String delimiters = "\t,;.?!-:@[](){}_*/";
    String[] stopWordsArray = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
        "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
        "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
        "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
        "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
        "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
        "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
        "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
        "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
        "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};

    void initialRandomGenerator(String seed) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA");
        messageDigest.update(seed.toLowerCase().trim().getBytes());
        byte[] seedMD5 = messageDigest.digest();

        long longSeed = 0;
        for (int i = 0; i < seedMD5.length; i++) {
            longSeed += ((long) seedMD5[i] & 0xffL) << (8 * i);
        }

        this.generator = new Random(longSeed);
    }

    Integer[] getIndexes() throws NoSuchAlgorithmException {
        Integer n = 10000;
        Integer number_of_lines = 50000;
        Integer[] ret = new Integer[n];
        this.initialRandomGenerator(this.userName);
        for (int i = 0; i < n; i++) {
            ret[i] = generator.nextInt(number_of_lines);
        }
        return ret;
    }

    public MP1(String userName, String inputFileName) {
        this.userName = userName;
        this.inputFileName = inputFileName;
    }

    public String[] process() throws Exception {
        final Set<String> stopWords = new HashSet<String>(Arrays.asList(stopWordsArray));
        final Map<String, Integer> counter = new HashMap<String, Integer>();
        final BufferedReader reader = new BufferedReader(new FileReader(inputFileName));
        final List<Integer> linenumbers = Arrays.asList(getIndexes());
        String line;
        Integer lineNumber = 0;
        final Map<Integer, String> lines = new HashMap<Integer, String>();
        while ((line = reader.readLine()) != null) {
            lines.put(lineNumber, line);
            lineNumber = lineNumber + 1;
        }

        for (final Integer index : linenumbers) {
            final String selectedLine = lines.get(index);
            final StringTokenizer tokenizer = new StringTokenizer(selectedLine, delimiters);
            while (tokenizer.hasMoreTokens()) {
                final String token = tokenizer.nextToken().toLowerCase().trim();
                if (stopWords.contains(token)) {
                    continue;
                }

                final Integer count = counter.get(token);
                if (count == null) {
                    counter.put(token, 1);
                } else {
                    counter.put(token, count + 1);
                }
            }
        }
        final List<Entry<String, Integer>> sortedEntries = new ArrayList<Entry<String, Integer>>();
        sortedEntries.addAll(counter.entrySet());
        Collections.sort(sortedEntries, new Comparator<Entry<String, Integer>>() {
            @Override
            public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
                final int v = o2.getValue().compareTo(o1.getValue());
                if (v != 0)
                    return v;
                
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        String[] ret = new String[20];
        for (int i = 0; i < 20; i++) {
            ret[i] = sortedEntries.get(i).getKey();
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("MP1 <User ID>");
        } else {
            String userName = args[0];
            String inputFileName = "./input.txt";
            MP1 mp = new MP1(userName, inputFileName);
            String[] topItems = mp.process();
            for (String item : topItems) {
                System.out.println(item);
            }
        }
    }
}
