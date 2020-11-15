package congreso.declaraciones;

import static java.lang.System.out;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

public class ImportadorDji {

  public static void main(String[] args) throws IOException {
    final var baseUrl = "https://dji.pide.gob.pe/consultas-dji/";
    var doc = Jsoup.connect(baseUrl + "dji-enti.php?ruc_enti=20161749126#personal").get();
    final var tables = doc.select("table.results");
    final var table = tables.first();
    System.out.println(table);
    final var headers = table.select("th").stream().map(Element::text).collect(toList());
    out.println(headers);

    var entities = new LinkedHashMap<String, Map<String, String>>();

    var trs = table.select("tr");
    for (int i = 1; i < trs.size(); i ++) {
      var r = trs.get(i);
      var cells = r.select("td");
      final var entidad = cells.get(1).text().trim();
      var entity = new LinkedHashMap<String, String>();
      entity.put("entidad", entidad);
      final var funcionario = cells.get(2).text().trim();
      entity.put("funcionario", funcionario);
      final var puesto = cells.get(3).text().trim();
      entity.put("puesto", puesto);
      final var element = cells.get(4);
      if (element.text().trim().equals("Falta DJI")) {
        entity.put("estado", "null");
      } else  {
        final var a = element.select("a").first();
        entity.put("estado", element.select("spam").first().text().trim().toUpperCase());
        entity.put("ref", baseUrl + a.attr("href"));
      }

      entities.put(funcionario, entity);
    }

    var builder = new StringBuilder();
    builder.append("ENTIDAD,PUESTO,NOMBRE_COMPLETO,DJI_ESTADO,DJI_REF").append("\n");
    entities.forEach((nombre, datos) -> builder.append(datos.get("entidad")).append(",")
        .append(datos.get("puesto")).append(",")
        .append(datos.get("funcionario")).append(",")
        .append(datos.get("estado")).append(",")
        .append(datos.get("ref")).append("\n"));
    Files.writeString(Path.of("static/congreso/dji/2020.csv"), builder.toString());

  }
}
