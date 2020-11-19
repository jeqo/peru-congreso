package congreso.leyes.exportador;

import static java.lang.System.out;
import static java.lang.Thread.sleep;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.StringValue;
import com.typesafe.config.ConfigFactory;
import congreso.leyes.Proyecto.ProyectoLey;
import congreso.leyes.internal.ProyectoIdSerde;
import congreso.leyes.internal.ProyectoLeySerde;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportadorTrello {

  static final ObjectMapper objectMapper = new ObjectMapper();

  static final String baseUrl = "https://api.trello.com";
  static final HttpClient client = HttpClient.newHttpClient();

  static final Logger LOG = LoggerFactory.getLogger(ExportadorTrello.class);
  public static final String API_TRELLO_COM = "https://api.trello.com";

  final String key, token;

  public ExportadorTrello(String key, String token) {
    this.key = key;
    this.token = token;
  }

  public static void main(String[] args) throws InterruptedException {
    var config = ConfigFactory.load();

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
        config.getString("kafka.consumer-groups.exportador-trello"));
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

    var proponentes = new LinkedHashSet<String>();
    var gruposParlamentarios = new LinkedHashSet<String>();
    var estados = new LinkedHashSet<String>();

    try (var iterator = proyectos.all()) {
      while (iterator.hasNext()) {
        var proyecto = (ProyectoLey) iterator.next().value;
        estados.add(proyecto.getEstado());
        proponentes.add(proyecto.getDetalle().getProponente());
        Optional.ofNullable(proyecto.getDetalle().getGrupoParlamentario())
            .map(StringValue::getValue)
            .filter(s -> !s.isBlank())
            .ifPresent(gruposParlamentarios::add);
      }
    }

    var key = config.getString("trello.key");
    var token = config.getString("trello.token");
    var boardId = config.getString("trello.board-id-open");
    var boardIdClosed = config.getString("trello.board-id-closed");

    var estadosClosed = Set
        .of("Publicado El Peruano", "Al Archivo", "Retirado", "Dictamen", "Dictamen Negativo",
            "Rechazado de Plano", "Autógrafa", "Aprobado", "Aprobado en Primera Votación");
    estados.removeAll(estadosClosed);

    var exportador = new ExportadorTrello(key, token);

    var estadoLists = exportador.prepareLists(boardId, estados);
//    var estadoClosedLists = exportador.prepareLists(boardIdClosed, estadosClosed);
    var proponentesLabels = exportador.prepareProponenteLabels(boardId, proponentes);
    var grupoLabels = exportador.prepareGrupoLabels(boardId, gruposParlamentarios);

    var lists = new LinkedHashMap<>(estadoLists);
//    lists.putAll(estadoClosedLists);

    out.println(estadoLists);
    out.println(proponentesLabels);
    out.println(grupoLabels);

    try {
      final var db = Path.of("static/trello/2016-2021.txt");
      var cards = Files.readAllLines(db)
          .stream()
          .collect(Collectors.toMap(s -> s.split(":")[0], s -> s.split(":")[1]));

      try (var iterator = proyectos.all()) {
        while (iterator.hasNext()) {
          var proyectoLey = (ProyectoLey) iterator.next().value;

          if (estados.contains(proyectoLey.getEstado())) {
            if (!cards.containsKey(proyectoLey.getId().getNumeroPeriodo())) {
              var title = String.format("%s - %s", proyectoLey.getDetalle().getNumeroUnico(),
                  proyectoLey.getTitulo().isBlank() ?
                      proyectoLey.getExpediente().getSubtitulo().getValue().toUpperCase() :
                      proyectoLey.getTitulo());

              var api = "/1/cards";
              var query = String.format("key=%s&token=%s",
                  key,
                  token);
              try {
                var desc = Optional.ofNullable(proyectoLey.getDetalle().getSumilla())
                    .map(StringValue::getValue)
                    .orElse("");
                final var idLabels = objectMapper.createArrayNode()
                    .add(proponentesLabels.get(proyectoLey.getDetalle().getProponente()));
                Optional.ofNullable(proyectoLey.getDetalle().getGrupoParlamentario())
                    .map(StringValue::getValue)
                    .filter(s -> !s.isBlank())
                    .map(grupoLabels::get)
                    .ifPresent(idLabels::add);
                var createCardRequestBody = objectMapper.createObjectNode()
                    .put("name", title)
                    .put("desc", desc)
                    .put("pos", "top")
                    .put("idList", lists.get(proyectoLey.getEstado()));
                createCardRequestBody.set("idLabels", idLabels);
                final var createCardUrl = baseUrl + api + "?" + query;
                out.println(createCardUrl);
                var createCard = HttpRequest.newBuilder()
                    .uri(URI.create(createCardUrl))
                    .POST(
                        BodyPublishers
                            .ofString(objectMapper.writeValueAsString(createCardRequestBody)))
                    .setHeader("Content-Type", "application/json")
                    .setHeader("Accept", "application/json")
                    .build();
                var createBoardResponse = client.send(createCard, BodyHandlers.ofString());
                out.println(createBoardResponse.body());
                var json = (ObjectNode) objectMapper.readTree(createBoardResponse.body());
                cards.put(proyectoLey.getId().getNumeroPeriodo(), json.get("id").textValue());
              } catch (IOException | InterruptedException e) {
                e.printStackTrace();
              }
            } else {
              var title = String.format("%s - %s", proyectoLey.getDetalle().getNumeroUnico(),
                  proyectoLey.getTitulo().isBlank() ?
                      proyectoLey.getExpediente().getSubtitulo().getValue().toUpperCase() :
                      proyectoLey.getTitulo());

              final var card = cards.get(proyectoLey.getId().getNumeroPeriodo());
              var api = "/1/cards/" + card;
              var query = String.format("key=%s&token=%s", key, token);
              try {
                var desc = Optional.ofNullable(proyectoLey.getDetalle().getSumilla())
                    .map(StringValue::getValue)
                    .orElse("");
                final var idLabels = objectMapper.createArrayNode()
                    .add(proponentesLabels.get(proyectoLey.getDetalle().getProponente()));
                Optional.ofNullable(proyectoLey.getDetalle().getGrupoParlamentario())
                    .map(StringValue::getValue)
                    .filter(s -> !s.isBlank())
                    .map(grupoLabels::get)
                    .ifPresent(idLabels::add);
                var createCardRequestBody = objectMapper.createObjectNode()
                    .put("name", title)
                    .put("desc", desc)
                    .put("pos", "top")
                    .put("idList", lists.get(proyectoLey.getEstado()));
                createCardRequestBody.set("idLabels", idLabels);
                final var createCardUrl = API_TRELLO_COM + api + "?" + query;
                out.println(createCardUrl);
                var createCard = HttpRequest.newBuilder()
                    .uri(URI.create(createCardUrl))
                    .POST(
                        BodyPublishers
                            .ofString(objectMapper.writeValueAsString(createCardRequestBody)))
                    .setHeader("Content-Type", "application/json")
                    .setHeader("Accept", "application/json")
                    .build();
                var createBoardResponse = client.send(createCard, BodyHandlers.ofString());
                out.println(createBoardResponse.body());
              } catch (IOException | InterruptedException e) {
                e.printStackTrace();
              }
            }
          }

          Files.writeString(db, cards.entrySet().stream()
              .map(e -> e.getKey() + ":" + e.getValue())
              .collect(Collectors.joining("\n")));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      kafkaStreams.close();
    }
  }

  Map<String, String> prepareProponenteLabels(String boardId, Set<String> proponentes) {
    var getLabelsApi = String.format("/1/boards/%s/labels", boardId);
    var getLabelsQuery = String.format("?key=%s", key);
    var getLabels = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + getLabelsApi + getLabelsQuery))
        .build();
    try {
      var response = client.send(getLabels, BodyHandlers.ofString());
      var json = objectMapper.readTree(response.body());
      var jsonArray = (ArrayNode) json;
      var labels = new LinkedHashMap<String, String>();
      jsonArray.elements().forEachRemaining(element -> {
        var jsonObject = (ObjectNode) element;
        labels.put(jsonObject.get("name").textValue(), jsonObject.get("id").textValue()
            //    + ":" + jsonObject.get("color").textValue()
        );
      });

      proponentes.stream()
          .filter(e -> !labels.containsKey(e))
          .forEach(listName -> {
            var api = String.format("/1/boards/%s/labels?", boardId);
            var query = String.format("key=%s&token=%s&color=yellow&name=",
                key,
                token);
            try {
              var labelNameEncoded = URLEncoder.encode(listName, StandardCharsets.UTF_8.name());
              var createList = HttpRequest.newBuilder()
                  .uri(URI.create(baseUrl + api + query + labelNameEncoded))
                  .POST(BodyPublishers.noBody())
                  .build();
              var createListResponse = client.send(createList, BodyHandlers.ofString());
              out.println(createListResponse.body());
            } catch (IOException | InterruptedException e) {
              e.printStackTrace();
            }
          });
      return labels;
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  Map<String, String> prepareGrupoLabels(String boardId, Set<String> grupos) {
    var getLabelsApi = String.format("/1/boards/%s/labels", boardId);
    var getLabelsQuery = String.format("?key=%s", key);
    var getLabels = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + getLabelsApi + getLabelsQuery))
        .build();
    try {
      var response = client.send(getLabels, BodyHandlers.ofString());
      var json = objectMapper.readTree(response.body());
      var jsonArray = (ArrayNode) json;
      var labels = new LinkedHashMap<String, String>();
      jsonArray.elements().forEachRemaining(element -> {
        var jsonObject = (ObjectNode) element;
        labels.put(jsonObject.get("name").textValue(), jsonObject.get("id").textValue()
            //    + ":" + jsonObject.get("color").textValue()
        );
      });

      out.println("Labels: " + labels);

      grupos.stream()
          .filter(e -> !labels.containsKey(e))
          .forEach(listName -> {
            var api = String.format("/1/boards/%s/labels?", boardId);
            var query = String.format("key=%s&token=%s&color=blue&name=",
                key,
                token);
            try {
              var labelNameEncoded = URLEncoder.encode(listName, StandardCharsets.UTF_8.name());
              var createList = HttpRequest.newBuilder()
                  .uri(URI.create(baseUrl + api + query + labelNameEncoded))
                  .POST(BodyPublishers.noBody())
                  .build();
              var createListResponse = client.send(createList, BodyHandlers.ofString());
              out.println(createListResponse.body());
            } catch (IOException | InterruptedException e) {
              e.printStackTrace();
            }
          });
      return labels;
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  Map<String, String> prepareLists(String boardId, Set<String> estados)
      throws InterruptedException {
    try {
      var getLists = HttpRequest.newBuilder()
          .uri(URI.create(baseUrl + String.format("/1/boards/%s/lists", boardId) + String
              .format("?key=%s", key)))
          .build();
      var getListsResponse = client.send(getLists, BodyHandlers.ofString());
      var jsonLists = (ArrayNode) objectMapper.readTree(getListsResponse.body());
      var lists = new LinkedHashMap<String, String>();
      jsonLists.elements().forEachRemaining(element -> {
        var jsonObject = (ObjectNode) element;
        lists.put(jsonObject.get("name").textValue(), jsonObject.get("id").textValue());
      });

      estados.stream()
          .filter(e -> !lists.containsKey(e))
          .forEach(listName -> {
            var api = String.format("/1/boards/%s/lists?", boardId);
            var query = String.format("key=%s&token=%s&name=",
                key,
                token);
            try {
              var listNameEncoded = URLEncoder.encode(listName, StandardCharsets.UTF_8.name());
              var createList = HttpRequest.newBuilder()
                  .uri(URI.create(baseUrl + api + query + listNameEncoded))
                  .POST(BodyPublishers.noBody())
                  .build();
              var createListResponse = client.send(createList, BodyHandlers.ofString());
              out.println(createListResponse.body());
            } catch (IOException | InterruptedException e) {
              e.printStackTrace();
            }
          });

      return (lists);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
