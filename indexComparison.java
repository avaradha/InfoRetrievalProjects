/**
 * IndexComparison class is used to run various analyzer for a
 * specified set of corpus.
 *
 * The Analyzers that are being compared are:
 *  1. StandardAnalyzer
 *  2. SimpleAnalyzer
 *  3. StopAnalyzer
 *  4. KeywordAnalyzer
 *
 * The program usage is given below:
 *
 * <tt>java IndexComparison <Corpus Directory> <Index Directory></tt>
 *
 * This code expects four different directories under the <Index Directory>
 * Four directories that are to be created are: <tt>StandardAnalyzer, SimpleAnalyzer, StopAnalyzer, KeywordAnalyzer</tt>
 *
 * @author Aravindh Varadharaju
 *
 */
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
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

public class indexComparison {

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


    public static final Analyzer[] analyzers = new Analyzer[]{
            new StandardAnalyzer(),
            new SimpleAnalyzer(),
            new StopAnalyzer(),
            new KeywordAnalyzer()
    };

    /**
     *
     * @param dataDir  - Directory where Corpus or data files are stored
     * @param indexDir - Directory where index files will be stored
     * @param analyzer - Type of Analyzer to be used for indexing activity
     * @throws IOException
     */
    public void index(String dataDir, String indexDir, Analyzer analyzer) throws IOException {
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
        String tmp = analyzer.getClass().getName();
        String analyzerName = tmp.substring(tmp.lastIndexOf(".")+1);
        String indexPath = indexDir+System.getProperty("file.separator")+analyzerName;
        System.out.println("------------------------------------");
        System.out.println("Analyzer Type: "+analyzerName);
        System.out.println("------------------------------------");
        Directory directory = FSDirectory.open(new File(indexPath));
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_0, analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter indexWriter = new IndexWriter(directory, iwc);
        if (dataFiles != null) {
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
        }
        indexWriter.forceMerge(1);
        indexWriter.commit();
        indexWriter.close();

    }

    /**
     *
     * @param indexDir  - Directory where index files will be stored
     * @param analyzer - Type of Analyzer to be used for indexing activity
     */

    public void getStats(String indexDir, Analyzer analyzer){
        IndexReader indexReader=null;
        try {
            String tmp = analyzer.getClass().getName();
            String analyzerName = tmp.substring(tmp.lastIndexOf(".")+1);
            String indexPath = indexDir+System.getProperty("file.separator")+analyzerName;
            indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
            System.out.print("\n");
            System.out.println("Total number of documents in the Corpus: " + indexReader.maxDoc());
            System.out.println("Number of documents containing the term \"new\" for field \"TEXT\": "+indexReader.docFreq(new Term("TEXT","new")));
            System.out.println("Number of occurences of \"new\" in the field \"TEXT\": "+indexReader.totalTermFreq(new Term("TEXT", "new")));
            Terms vocabulary = MultiFields.getTerms(indexReader, "TEXT");
            System.out.println("Size of the vocabulary for this field: "+vocabulary.size());
            System.out.println("Number of documents that have at least one term for this field: "+vocabulary.getDocCount());
            System.out.println("Number of tokens for this field: "+vocabulary.getSumTotalTermFreq());
            System.out.println("Number of postings for this field: "+vocabulary.getSumDocFreq());
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
            System.out.println("Usage: java " + indexComparison.class.getName() + " <Corpus Directory> <Index Directory>");
            System.exit(-1);
        }
        String docDir = args[0];    // Path where the corpus is stored
        String indexDir = args[1];  // Path where index files will be written to
        long startTime = System.currentTimeMillis();
        indexComparison obj = new indexComparison();
        for (Analyzer analyzer : analyzers) {
            try {
                obj.index(docDir, indexDir, analyzer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            obj.getStats(indexDir, analyzer);
        }
        long endTime = System.currentTimeMillis();
        long millis = endTime - startTime;
        System.out.print("Time taken: "+String.format("%d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(millis),
                TimeUnit.MILLISECONDS.toSeconds(millis) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
        ));
    }
}
