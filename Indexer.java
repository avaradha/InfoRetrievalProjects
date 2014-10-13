/**
 * Indexer class is used to run the StandardAnalyzer for a
 * specified set of corpus. The program usage is given below:
 *
 * <tt>java Indexer <Corpus Directory> <Index Directory></tt>
 *
 * This code expects a directory <tt>StandardAnalyzer</tt> under the <tt><Index Directory></tt>
 * The directory that is to be created is:
 *
 * @author Aravindh Varadharaju
 *
 */
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.*;
import java.util.concurrent.TimeUnit;

import static org.apache.lucene.document.Field.Store.YES;

//import org.apache.lucene.util.BytesRef;

public class Indexer {

    public static final String DOC = "<DOC>";
    public static final String TERMINATING_DOC = "</DOC>";
    public static final String DOCNO = "<DOCNO>";
    public static final String TERMINATING_DOCNO = "</DOCNO>";
    public static final String HEAD = "<HEAD>";
    public static final String TERMINATING_HEAD = "</HEAD>";
    public static final String BYLINE = "<BYLINE>";
    public static final String TERMINATING_BYLINE = "</BYLINE>";
    public static final String DATELINE = "<DATELINE>";
    public static final String TERMINATING_DATELINE = "</DATELINE>";
    public static final String TEXT = "<TEXT>";
    public static final String TERMINATING_TEXT = "</TEXT>";

    /**
     *
     * @param dataDir  - Directory where Corpus or data files are stored
     * @param indexDir - Directory where index files will be stored
     * @throws IOException
     */

    public void index(String dataDir, String indexDir) throws IOException {
        File[] dataFiles = new File(dataDir).listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                boolean extn = false;
                if (file.getName().toLowerCase().endsWith(".trectext"))
                {
                    extn = true;
                }
                return extn;
            }
        });
        System.out.println("**************************************************************************");
        System.out.println("Number of files to be processed: "+(dataFiles.length));
        Directory directory = FSDirectory.open(new File(indexDir));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_0, analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter indexWriter = new IndexWriter(directory, iwc);
        int fileNo=1;
        for (File file : dataFiles) {
            System.out.print("\r" + "Indexing file # " + Integer.toString(fileNo) + " : " + file.toString());
            fileNo++;
            FileInputStream fis = null;
            BufferedInputStream bis = null;
            DataInputStream dis = null;
            String fileContent;
            try {
                StringBuilder sbFile = new StringBuilder();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                while (bufferedReader.ready()) {
                    sbFile.append(bufferedReader.readLine());
                }
                fileContent = sbFile.toString();
                String[] docs = StringUtils.substringsBetween(fileContent, DOC, TERMINATING_DOC);
                for (String doc : docs) {
                    Document luceneDoc = new Document();
                    //System.out.println("---------------------------");
                    //System.out.println(doc);
                    String[] docno_array = StringUtils.substringsBetween(doc, DOCNO, TERMINATING_DOCNO);
                    String docno = StringUtils.join(docno_array, " ");
                    //System.out.println("DOC NO: "+docno);
                    if (docno != null)
                        luceneDoc.add(new StringField("DOCNO", docno, YES));
                    //
                    String[] head_array = StringUtils.substringsBetween(doc, HEAD, TERMINATING_HEAD);
                    String head;
                    head = StringUtils.join(head_array, " ");
                    //System.out.println("HEAD: "+head);
                    if (head != null)
                        luceneDoc.add(new TextField("HEAD", head, YES));
                    //
                    String[] bylines_array = StringUtils.substringsBetween(doc, BYLINE, TERMINATING_BYLINE);
                    String bylines = StringUtils.join(bylines_array, " ");
                    //System.out.println("BYLINE: "+bylines);
                    if (bylines != null)
                        luceneDoc.add(new TextField("BYLINE", bylines, YES));
                    //
                    String[] dateline_array = StringUtils.substringsBetween(doc, DATELINE, TERMINATING_DATELINE);
                    String dateline = StringUtils.join(dateline_array, " ");
                    //System.out.println("DATELINE: "+dateline);
                    if (dateline != null)
                        luceneDoc.add(new TextField("DATELINE", dateline, YES));
                    //
                    String[] text_array = StringUtils.substringsBetween(doc, TEXT, TERMINATING_TEXT);
                    //System.out.println("TEXT: "+text);
                    String text = StringUtils.join(text_array, " ");
                    if (text != null)
                        luceneDoc.add(new TextField("TEXT", text, YES));
                    indexWriter.addDocument(luceneDoc);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (dis != null) {
                        dis.close();
                    }
                    if (bis != null) {
                        bis.close();
                    }
                    if (fis != null) {
                        fis.close();
                    }
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
        indexWriter.forceMerge(1);
        indexWriter.commit();
        indexWriter.close();

    }

    /**
     *
     * @param indexDirPath - Directory where index files will be stored
     */

    public void getStats(String indexDirPath){
        IndexReader indexReader=null;
        try {
            indexReader = DirectoryReader.open(FSDirectory.open(new File(indexDirPath)));
            System.out.print("\n");
            System.out.println("**************************************************************************");
            System.out.println("Total number of documents in the Corpus: "+indexReader.maxDoc());
            System.out.println("Number of documents containing the term \"new\" for field \"TEXT\": "+indexReader.docFreq(new Term("TEXT","new")));
            System.out.println("Number of occurences of \"new\" in the field \"TEXT\": "+indexReader.totalTermFreq(new Term("TEXT", "new")));
            Terms term = MultiFields.getTerms(indexReader, "TEXT");
            System.out.println("Size of the vocabulary for this field: " + term.size());
            System.out.println("Number of documents that have at least one term for this field: " + term.getDocCount());
            System.out.println("Number of tokens for this field: "+term.getSumTotalTermFreq());
            System.out.println("Number of postings for this field: " + term.getSumDocFreq());
            System.out.println("**************************************************************************");
            /*
            TermsEnum iterator = vocabulary.iterator(null);
            System.out.println("\n*******Vocabulary-Start**********");
            BytesRef byteRef;
            while((byteRef = iterator.next()) != null) {
                String term = byteRef.utf8ToString();
                //printWriter.append(term+"\n");
                //System.out.print(term+"\t");
            }
            System.out.println("\n*******Vocabulary-End**********");
            */
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (indexReader != null) {
                    indexReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
	// write your code here
        if(args.length != 2) {
            System.out.println("Usage: java " + Indexer.class.getName() + " <Corpus Directory> <Index Directory>");
            System.exit(-1);
        }
        String docDir = args[0];    // Path where the corpus is stored
        String indexDir = args[1];  // Path where index files will be written to
        long startTime = System.currentTimeMillis();
        Indexer obj = new Indexer();
        try {
            obj.index(docDir, indexDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        obj.getStats(indexDir);
        long endTime = System.currentTimeMillis();
        long millis = endTime - startTime;
        System.out.print("Time taken: "+String.format("%d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(millis),
                TimeUnit.MILLISECONDS.toSeconds(millis) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
        ));
    }
}
