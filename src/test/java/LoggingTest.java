import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.junit.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;

public class LoggingTest {

    @Test
    public void runDUUI() throws Exception{

        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText("Mensch (Homo sapiens, lateinisch für „verstehender, verständiger“ oder „weiser, gescheiter, kluger, vernünftiger Mensch“) ist nach der biologischen Systematik eine Art der Gattung Homo aus der Familie der Menschenaffen, die zur Ordnung der Primaten und damit zu den Höheren Säugetieren gehört.");
        jCas.setDocumentLanguage("de");

        int iWorkers = 1;
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(ctx)
                .withWorkers(1)
                .withDebugLevel(DUUIComposer.DebugLevel.TRACE)  // Add logger and set what to log
                //.withComponentLogging(false)  // Can be used to disable component logging
                .withDebugColorful(true)  // Make logs colorful: warnings red, ... (Default is true)
                .withDebugSeverity(true)  // Print log level next to message (Default is true)
                .withDebugSource(true)  // Print driver- and document name next to message (Default is true)
                .withStorageBackend(new DUUISqliteStorageBackend("test.db"));

        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(uima_driver, remoteDriver, dockerDriver);

        composer.add(new DUUIRemoteDriver.Component("http://localhost:25591")
                .withWorkers(1)
                .withTargetView("out")
                .withName("MyComp")
                .build());

        composer.run(jCas, "Test");
    }

}
