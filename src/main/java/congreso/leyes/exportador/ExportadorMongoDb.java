package congreso.leyes.exportador;

import static java.lang.Thread.sleep;
import static java.util.stream.Collectors.toList;

import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.typesafe.config.ConfigFactory;
import congreso.leyes.Proyecto.ProyectoLey;
import congreso.leyes.internal.ProyectoIdSerde;
import congreso.leyes.internal.ProyectoLeySerde;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.Stores;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportadorMongoDb {

  static final Logger LOG = LoggerFactory.getLogger(ExportadorMongoDb.class);

  public static void main(String[] args) throws InterruptedException {
    var config = ConfigFactory.load();

    final var url = config.getString("mongodb.url");
    var uri = new MongoClientURI(url);
    var mongoClient = new MongoClient(uri);
    var db = mongoClient.getDatabase(config.getString("mongodb.database"));
    var collection = db.getCollection(config.getString("mongodb.collection.resumen"));
    var expCollection = db.getCollection(config.getString("mongodb.collection.expediente"));
    var segCollection = db.getCollection(config.getString("mongodb.collection.seguimiento"));

    var kafkaBootstrapServers = config.getString("kafka.bootstrap-servers");
    var topicExpedientes = config.getString("kafka.topics.expediente-importado");

    LOG.info("Cargando proyectos importados");

    var builder = new StreamsBuilder();
    builder.globalTable(topicExpedientes,
        Consumed.with(new ProyectoIdSerde(), new ProyectoLeySerde())
            .withOffsetResetPolicy(AutoOffsetReset.EARLIEST),
        Materialized.as(Stores.persistentKeyValueStore("proyectos")));

    var streamsConfig = new Properties();
    streamsConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        config.getString("kafka.consumer-groups.exportador-mongodb"));
    var streamsOverrides = config.getConfig("kafka.streams").entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().unwrapped()));
    streamsConfig.putAll(streamsOverrides);

    var kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
    kafkaStreams.start();

    while (!kafkaStreams.state().isRunningOrRebalancing()) {
      LOG.info("Esperando por streams a cargar...");
      sleep(Duration.ofSeconds(1).toMillis());
    }

    var proyectos = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("proyectos", QueryableStoreTypes.keyValueStore()));

    var docs = new ArrayList<Document>();
    var expDocs = new ArrayList<Document>();
    var segDocs = new ArrayList<Document>();
    try (var iter = proyectos.all()) {
      while (iter.hasNext()) {
        var proyectoLey = (ProyectoLey) iter.next().value;
        var doc = toResumenDoc(proyectoLey);
        var expDoc = toExpedienteDoc(proyectoLey);
        var segDoc = toSeguimientoDoc(proyectoLey);
        docs.add(doc);
        expDocs.add(expDoc);
        segDocs.add(segDoc);
      }
    }
    collection.insertMany(docs);
    segCollection.insertMany(segDocs);
    expCollection.insertMany(expDocs);

    LOG.info("Exportacion a MongoDB completado");

    mongoClient.close();
    kafkaStreams.close();
  }

  static Document toResumenDoc(ProyectoLey proyectoLey) {
    Document doc = new Document();
    doc.append("_id", proyectoLey.getDetalle().getNumeroUnico());
    doc.append("estado", proyectoLey.getEstado());
    doc.append("periodo", proyectoLey.getId().getPeriodo());
    doc.append("numero_periodo", proyectoLey.getId().getNumeroPeriodo());
    doc.append("titulo", proyectoLey.getDetalle().getTitulo());
    doc.append("sumilla",
        Optional.ofNullable(proyectoLey.getDetalle().getSumilla()).map(StringValue::getValue)
            .orElse(""));
    doc.append("fecha_publicacion", proyectoLey.getFechaPublicacion());
    doc.append("fecha_actualizacion", Optional.ofNullable(proyectoLey.getFechaActualizacion())
        .map(Int64Value::getValue).orElse(null));
    doc.append("proponente", proyectoLey.getDetalle().getProponente());
    doc.append("grupo_parlamentario",
        Optional.ofNullable(proyectoLey.getDetalle().getGrupoParlamentario())
            .map(StringValue::getValue).orElse(""));
    doc.append("iniciativas_agrupadas",
        new ArrayList<>(proyectoLey.getDetalle().getIniciativaAgrupadaList()));
    doc.append("autores",
        new ArrayList<>(proyectoLey.getDetalle().getAutorList()));
    doc.append("adherentes",
        new ArrayList<>(proyectoLey.getDetalle().getAdherenteList()));
    doc.append("sectores", new ArrayList<>(proyectoLey.getDetalle().getSectorList()));
    if (proyectoLey.getEnlaces() != null) {
      var enlBson = new BasicDBObject();
      enlBson.append("expediente", proyectoLey.getEnlaces().getExpediente());
      enlBson.append("seguimiento", proyectoLey.getEnlaces().getSeguimiento());
      enlBson.append("opiniones_publicadas", proyectoLey.getEnlaces().getOpinionesPublicadas());
      enlBson.append("publicar_opinion",
          Optional.ofNullable(proyectoLey.getEnlaces().getPublicarOpinion()).map(
              StringValue::getValue).orElse(""));
      doc.append("enlaces", enlBson);
    }
    if (proyectoLey.getLey() != null) {
      var docLey = new BasicDBObject();
      docLey.append("numero", proyectoLey.getLey().getNumero());
      docLey.append("titulo", proyectoLey.getLey().getTitulo());
      docLey.append("sumilla", proyectoLey.getLey().getSumilla().getValue());
      doc.append("ley", docLey);
    }
    return doc;
  }

  static Document toSeguimientoDoc(ProyectoLey proyectoLey) {
    Document doc = new Document();
    doc.append("_id", proyectoLey.getDetalle().getNumeroUnico());
    doc.append("seguimientos", proyectoLey.getSeguimientoList().stream()
        .map(s -> {
          var bson = new BasicDBObject();
          bson.append("fecha", s.getFecha());
          bson.append("texto", s.getTexto());
          return bson;
        })
        .collect(toList()));
    return doc;
  }

  static Document toExpedienteDoc(ProyectoLey proyectoLey) {
    Document doc = new Document();
    doc.append("_id", proyectoLey.getDetalle().getNumeroUnico());
    doc.append("resultado", proyectoLey.getExpediente().getResultadoList()
        .stream()
        .map(ExportadorMongoDb::toResumenDoc)
        .collect(toList()));
    doc.append("proyecto", proyectoLey.getExpediente().getProyectoList()
        .stream()
        .map(ExportadorMongoDb::toResumenDoc)
        .collect(toList()));
    doc.append("anexo", proyectoLey.getExpediente().getAnexoList()
        .stream()
        .map(ExportadorMongoDb::toResumenDoc)
        .collect(toList()));
    return doc;
  }

  private static BasicDBObject toResumenDoc(ProyectoLey.Expediente.Documento documento) {
    var bson = new BasicDBObject();
    bson.append("titulo", documento.getTitulo());
    bson.append("url", documento.getUrl());
    bson.append("fecha",
        Optional.ofNullable(documento.getFecha()).map(Int64Value::getValue).orElse(null));
    return bson;
  }
}
