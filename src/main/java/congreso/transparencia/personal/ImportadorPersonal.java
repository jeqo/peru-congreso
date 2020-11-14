package congreso.transparencia.personal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import org.jsoup.Jsoup;

public class ImportadorPersonal {

  public static void main(String[] args) throws IOException {
    var html = Files.readString(Path.of("static/transparencia/personal/download.html"));
    var doc = Jsoup.parse(html);
    var rows = doc.select("tr");
    var headers = rows.first().select("td").stream()
        .map(h -> h.text().trim().toUpperCase())
        .collect(Collectors.toList());
    var buffer = new StringBuilder();
    var csvHeader = String.join(",", headers);
    buffer.append(csvHeader).append("\n");
    for(int i = 1; i < rows.size(); i++) {
      var row = rows.get(i).select("td").stream()
          .map(h -> h.text().trim().toUpperCase())
          .collect(Collectors.toList());
      var csvRow = String.join(",", row);
      buffer.append(csvRow).append("\n");
    }
    Files.writeString(Path.of("static/transparencia/personal/2020-09.csv"), buffer.toString());
  }
}
