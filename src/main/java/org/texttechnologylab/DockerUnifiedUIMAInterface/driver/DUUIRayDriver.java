package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

/**
 * Driver for Ray-based ML training components.
 *
 * This driver handles the special requirements of Ray components that need to collect
 * training data from multiple workers before starting a single training process.
 *
 * @author Daniel Bundan
 */
public class DUUIRayDriver implements IDUUIDriverInterface {


    // Static map to coordinate training across all RayDriver instances
    // Key: component URL, Value: training data collector
    private static final ConcurrentHashMap<String, GlobalTrainingSession> _globalTrainingSessions = new ConcurrentHashMap<>();

    private HashMap<String, InstantiatedRayComponent> _activeComponents;
    private HttpClient _httpClient;
    private DUUILuaContext _luaContext;
    private int _timeout;

    public DUUIRayDriver() {
        _activeComponents = new HashMap<>();
        _httpClient = HttpClient.newHttpClient();
        _timeout = 30000; // 30 seconds default timeout
    }

    public DUUIRayDriver withTimeout(int timeoutMilliseconds) {
        _timeout = timeoutMilliseconds;
        return this;
    }

    @Override
    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent component) throws InvalidXMLException, IOException, SAXException {
        // Ray components are identified by having a Ray URL or specific parameter
        return component.getUrl() != null &&
                (component.getUrl().get(0).contains("ray") ||
                        component.getParameters().containsKey("ray_component") ||
                        component.getParameters().containsKey("training_component"));
    }

    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_activeComponents.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }

        String rayUrl = component.getUrl().get(0);

        // Get or create global training session for this Ray component URL
        GlobalTrainingSession trainingSession = _globalTrainingSessions.computeIfAbsent(rayUrl,
                url -> new GlobalTrainingSession(url));

        // Verify Ray component is responsive
        if (!skipVerification) {
            verifyRayComponentResponsive(rayUrl);
        }

        // Get communication layer from Ray component
        IDUUICommunicationLayer communicationLayer = getRayComponentCommunicationLayer(rayUrl, jc);

        InstantiatedRayComponent rayComponent = new InstantiatedRayComponent(component, trainingSession);
        rayComponent.setCommunicationLayer(communicationLayer);
        rayComponent.setUrl(rayUrl);

        _activeComponents.put(uuid, rayComponent);

        System.out.printf("[RayDriver][%s] Ray component instantiated at %s\n", uuid, rayUrl);
        return uuid;
    }

    @Override
    public void run(String uuid, JCas cas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer)
            throws CASException, PipelineComponentException, CompressorException, IOException, InterruptedException, SAXException, CommunicationLayerException {


        System.out.println("testteststetststetst");

        InstantiatedRayComponent component = _activeComponents.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the Ray Driver");
        }

        submitTrainingData(component, cas);

        receiveTrainingResults(component, cas);

        // Ray training workflow:
        // 1. Collect training data from this worker
        // 2. Wait for all workers to submit their data
        // 3. Trigger training once all data is collected
        // 4. Return training results
    }

    @Override
    public TypeSystemDescription get_typesystem(String uuid)
            throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {

        InstantiatedRayComponent component = _activeComponents.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the Ray Driver");
        }

        return IDUUIInstantiatedPipelineComponent.getTypesystem(uuid, component);
    }

    @Override
    public void printConcurrencyGraph(String uuid) {
        InstantiatedRayComponent component = _activeComponents.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the Ray Driver");
        }
        System.out.printf("[RayDriver][%s]: Ray training component (singleton)\n", uuid);
    }

    @Override
    public int initReaderComponent(String uuid, Path filePath) throws Exception {
        throw new UnsupportedOperationException("Ray components do not support reader functionality");
    }

    @Override
    public boolean destroy(String uuid) {
        InstantiatedRayComponent component = _activeComponents.remove(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the Ray Driver");
        }

        // Clean up any training resources
        component.cleanup();

        // Also cleanup the global training session if this is the last component using it
        String componentUrl = component.getUrl();
        if (componentUrl != null) {
            _globalTrainingSessions.computeIfPresent(componentUrl, (url, session) -> {
                session.cleanup();
                return null; // Remove from map
            });
        }

        System.out.printf("[RayDriver][%s] Ray component destroyed\n", uuid);
        return true;
    }

    public void shutdown() {
        for (String uuid : new HashSet<>(_activeComponents.keySet())) {
            destroy(uuid);
        }
    }

    /**
     * Collects training data from a worker and manages the training coordination
     */
    private void submitTrainingData(InstantiatedRayComponent component, JCas cas)
            throws CommunicationLayerException, CASException, IOException, InterruptedException {

        // Serialize CAS data for training
        //ByteArrayOutputStream trainingDataStream = new ByteArrayOutputStream();  // Data as bytes
        //component.getCommunicationLayer().serialize(cas, trainingDataStream, component.getParameters());

        // Submit this worker's data to the global training session
        component.getTrainingSession().submitTrainingData(cas);
    }

    /**
     * Waits for training to complete and retrieves results
     */
    private void receiveTrainingResults(InstantiatedRayComponent component, JCas cas)
            throws InterruptedException, CommunicationLayerException, CASException, IOException {

        GlobalTrainingSession trainingSession = component.getTrainingSession();

        // Check if this worker should trigger the training (first to reach the threshold)
        if (trainingSession.shouldTriggerTraining()) {
            triggerRayTraining(trainingSession);
        }

        // Wait for training to finish
        trainingSession.awaitTrainingCompletion();

        // Get training results and deserialize back to CAS
        byte[] trainingResults = trainingSession.getTrainingResults();
        if (trainingResults != null) {
            ByteArrayInputStream resultStream = new ByteArrayInputStream(trainingResults);
            component.getCommunicationLayer().deserialize(cas, resultStream);
        }
    }

    /**
     * Triggers the actual Ray training process
     */
    private void triggerRayTraining(GlobalTrainingSession trainingSession) throws IOException, InterruptedException {
        System.out.println("Worker triggered training.");

        String rayUrl = trainingSession.getRayUrl();

        StringBuilder s = new StringBuilder();

        for(JCas cas : trainingSession.getAllTrainingData()){
            if(s.toString().equals("")){
                s.append(cas.getSofaDataString());
            }else{
                s.append("\n\n").append(cas.getSofaDataString());
            }
        }

        // Send start training signal to Ray component with all collected data
        HttpRequest trainingRequest = HttpRequest.newBuilder()
                .uri(URI.create(rayUrl + "/v1/train"))
                .POST(HttpRequest.BodyPublishers.ofString(s.toString()))
                .build();

        HttpResponse<byte[]> response = _httpClient.send(trainingRequest, HttpResponse.BodyHandlers.ofByteArray());

        if (response.statusCode() == 200) {
            trainingSession.setTrainingResults(response.body());
            trainingSession.markTrainingComplete();
            System.out.printf("[RayDriver] Training completed successfully\n");
        } else {
            throw new IOException(format("Ray training failed with status code: %d", response.statusCode()));
        }
    }

    /**
     * Verifies that the Ray component is responsive
     */
    private void verifyRayComponentResponsive(String rayUrl) throws IOException, InterruptedException {
        HttpRequest healthRequest = HttpRequest.newBuilder()
                .uri(URI.create(rayUrl + "/v1/health"))
                .GET()
                .build();

        HttpResponse<String> response = _httpClient.send(healthRequest, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException(format("Ray component at %s is not responsive", rayUrl));
        }
    }

    /**
     * Gets the communication layer from the Ray component
     */
    private IDUUICommunicationLayer getRayComponentCommunicationLayer(String rayUrl, JCas jc)
            throws IOException, InterruptedException {

        HttpRequest layerRequest = HttpRequest.newBuilder()
                .uri(URI.create(rayUrl + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER))
                .GET()
                .build();

        HttpResponse<String> response = _httpClient.send(layerRequest, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
            // Create communication layer from Lua script (similar to other drivers)
            return new DUUILuaCommunicationLayer(response.body(), "ray_component", _luaContext);
        } else {
            throw new IOException(format("Failed to get communication layer from Ray component at %s", rayUrl));
        }
    }

    /**
     * Component wrapper for Ray-based training components
     */
    public static class Component {
        private DUUIPipelineComponent component;

        public Component(String rayUrl) throws URISyntaxException, IOException {
            component = new DUUIPipelineComponent();
            component.withUrl(rayUrl);
            component.withParameter("ray_component", "true");
        }

        public Component(DUUIPipelineComponent pipelineComponent) {
            component = pipelineComponent;
            component.withParameter("ray_component", "true");
        }

        public Component withParameter(String key, String value) {
            component.withParameter(key, value);
            return this;
        }

        public Component withDescription(String description) {
            component.withDescription(description);
            return this;
        }

        public Component withView(String viewName) {
            component.withView(viewName);
            return this;
        }

        public Component withSourceView(String viewName) {
            component.withSourceView(viewName);
            return this;
        }

        public Component withTargetView(String viewName) {
            component.withTargetView(viewName);
            return this;
        }

        // Ray components should always have scale=1, but we provide this for API consistency
        public Component withScale(int scale) {
            if (scale != 1) {
                System.out.println("[RayDriver] Warning: Ray components always use scale=1, ignoring scale parameter");
            }
            component.withScale(1);
            return this;
        }

        public DUUIPipelineComponent build() {
            component.withDriver(DUUIRayDriver.class);
            component.withScale(1); // Force scale=1 for Ray components
            return component;
        }
    }

    /**
     * Ray-specific instantiated component that handles training data collection and coordination
     */
    private static class InstantiatedRayComponent implements IDUUIInstantiatedPipelineComponent {
        private final DUUIPipelineComponent _component;
        private final ConcurrentLinkedQueue<ComponentInstance> _components;
        private final GlobalTrainingSession _trainingSession;
        private IDUUICommunicationLayer _communicationLayer;
        private String _rayUrl;

        private final int _scale;
        private final boolean _websocket;
        private final int _ws_elements;

        private final Map<String, String> _parameters;
        private String _sourceView;
        private String _targetView;
        private String sHost = "localhost";

        public InstantiatedRayComponent(DUUIPipelineComponent comp, GlobalTrainingSession trainingSession) {
            _component = comp;
            _components = new ConcurrentLinkedQueue<>();
            _trainingSession = trainingSession;

            _parameters = comp.getParameters();
            _targetView = comp.getTargetView();
            _sourceView = comp.getSourceView();
            _scale = comp.getScale(1);

            _websocket = comp.isWebsocket();
            _ws_elements = comp.getWebsocketElements();
        }

        public void setCommunicationLayer(IDUUICommunicationLayer layer) {
            _communicationLayer = layer;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communicationLayer;
        }

        public void setUrl(String url) {
            _rayUrl = url;
        }

        public String getUrl() {
            return _rayUrl;
        }

        @Override
        public DUUIPipelineComponent getPipelineComponent() {
            return _component;
        }

        @Override
        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            ComponentInstance inst = _components.poll();
            while (inst == null) {
                inst = _components.poll();
            }
            long mutexEnd = System.nanoTime();
            return Triplet.with(inst, mutexStart, mutexEnd);
        }

        @Override
        public void addComponent(IDUUIUrlAccessible item) {
            _components.add((ComponentInstance) item);
        }

        public Map<String, String> getParameters() {
            return _component.getParameters();
        }

        @Override
        public String getSourceView() {
            return _sourceView;
        }

        @Override
        public String getTargetView() {
            return _targetView;
        }

        @Override
        public String getUniqueComponentKey() {
            return "";
        }

        public GlobalTrainingSession getTrainingSession() {
            return _trainingSession;
        }

        public void cleanup() {
            _trainingSession.cleanup();
        }
    }


    private static class ComponentInstance implements IDUUIUrlAccessible {
        String _host;
        IDUUIConnectionHandler _handler;
        IDUUICommunicationLayer _communication_layer;

        public ComponentInstance(String url, IDUUICommunicationLayer layer) {
            _host = url;
            _communication_layer = layer;
        }

        public IDUUICommunicationLayer getCommunicationLayer() {
            return _communication_layer;
        }

        public ComponentInstance(String url, IDUUICommunicationLayer layer, IDUUIConnectionHandler handler) {
            _host = url;
            _communication_layer = layer;
            _handler = handler;
        }

        public String generateURL() {
            return _host;
        }

        public IDUUIConnectionHandler getHandler() {
            return _handler;
        }
    }

    /**
     * Global training session coordinator that manages training data collection
     * and synchronization across all workers in the pipeline
     */
    private static class GlobalTrainingSession {
        private final String _rayUrl;
        private final List<JCas> _trainingDataBatches;
        private final AtomicInteger _expectedWorkers;
        private final AtomicInteger _receivedBatches;
        private final CountDownLatch _trainingCompleteLatch;
        private final AtomicBoolean _trainingTriggered;
        private byte[] _trainingResults;
        private final Object _trainingLock = new Object();

        public GlobalTrainingSession(String rayUrl) {
            _rayUrl = rayUrl;
            _trainingDataBatches = Collections.synchronizedList(new ArrayList<>());
            _expectedWorkers = new AtomicInteger(1); // Will be updated based on pipeline worker count
            _receivedBatches = new AtomicInteger(0);
            _trainingCompleteLatch = new CountDownLatch(1);
            _trainingTriggered = new AtomicBoolean(false);
        }

        public void initializeWorkerCount(int workerCount) {
            _expectedWorkers.compareAndSet(1, workerCount);
        }

        public void submitTrainingData(JCas data) {
            _trainingDataBatches.add(data);
            int received = _receivedBatches.incrementAndGet();
            System.out.printf("[RayDriver] Received training data batch %d/%d\n", received, _expectedWorkers.get());
        }

        public boolean shouldTriggerTraining() {
            // Trigger training with only 1 worker
            return _receivedBatches.get() >= _expectedWorkers.get() &&
                    _trainingTriggered.compareAndSet(false, true);
        }

        public List<JCas> getAllTrainingData() {
            return _trainingDataBatches;
        }

        public void setTrainingResults(byte[] results) {
            synchronized (_trainingLock) {
                _trainingResults = results;
            }
        }

        public byte[] getTrainingResults() {
            synchronized (_trainingLock) {
                return _trainingResults;
            }
        }

        public void markTrainingComplete() {
            _trainingCompleteLatch.countDown();
        }

        public void awaitTrainingCompletion() throws InterruptedException {
            _trainingCompleteLatch.await();
        }

        public String getRayUrl() {
            return _rayUrl;
        }

        public void cleanup() {
            _trainingDataBatches.clear();
            _trainingResults = null;
        }
    }
}