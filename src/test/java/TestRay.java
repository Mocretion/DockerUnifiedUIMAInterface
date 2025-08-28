import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.core.io.xmi.XmiWriter;
import org.hucompute.textimager.uima.type.category.CategoryCoveredTagged;
import org.junit.Test;
import org.luaj.vm2.LuaError;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIMultimodalCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;
import org.texttechnologylab.annotation.GNMetaData;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class TestRay {

    @Test
    public void composerTest() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();

        //CasIOUtils.save(aCas.getCas(), new FileOutputStream(new File("/tmp/audiotest.xmi")), SerialFormat.XMI_1_1);
        int iWorkers = 1;

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(1);         // wir geben dem Composer eine Anzahl an Threads mit.

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIRayDriver rayDriver = new DUUIRayDriver();


        //DUUIYouTubeReader ytReader = new DUUIYouTubeReader("https://www.youtube.com/@Jules1/videos", "AIzaSyDycLCdJ1_jfkFL-pWnQuf1FzluJbX21Bw");
        //DUUIYouTubeReader ytReader = new DUUIYouTubeReader("https://www.youtube.com/watch?v=SV6NJ6PcGBs&list=PLh19WWr20745LHdlDAg2P_JT7I2Wx6axP", "AIzaSyDycLCdJ1_jfkFL-pWnQuf1FzluJbX21Bw");
        DUUIMultimodalCollectionReader multiReader = new DUUIMultimodalCollectionReader("/home/staff_homes/bundan/raytestcsv", "csv");

        Set<DUUICollectionReader> readers = new HashSet<>();

        //readers.add(ytReader);
        readers.add(multiReader);

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(readers);

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(uima_driver, remoteDriver, rayDriver);

        composer.add(new DUUIRayDriver.Component("http://isengart.hucompute.org:25580")  // Annotheia
                .withScale(1)
                .build());

        composer.run(processor, "test");


        //composer.run(aCas);
    }
}
