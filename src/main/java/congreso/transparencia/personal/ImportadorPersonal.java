package congreso.transparencia.personal;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import org.jsoup.Jsoup;

public class ImportadorPersonal {

  public static void main(String[] args) throws IOException {
    var path = Path.of("static/transparencia/personal/download.html");
    var html = Files.readString(path, Charset.defaultCharset());
    var doc = Jsoup.parse(html);
    var rows = doc.select("tr");
    var headers = rows.first().select("td").stream()
        .map(h -> h.text().trim().toUpperCase())
        .map(n -> n
            .replace("PK_", "")
            .replace("VC_PERSONAL_", "")
            .replace("MO_PERSONAL_", "")
            .replace("IN_PERSONAL_", ""))
        .collect(Collectors.toList());
    var cols = headers.size();
    System.out.println("Cols:" + cols);
    var buffer = new StringBuilder();
    var csvHeader = String.join(",", headers);
    buffer.append(csvHeader).append("\n");
    for (int i = 1; i < rows.size(); i++) {
      var row = rows.get(i).select("td").stream()
          .map(h -> h.text().trim().toUpperCase()
              .replace(",", ""))
          .collect(Collectors.toList());
      var csvRow = String.join(",", row);
      buffer.append(csvRow).append("\n");
    }
    Files.writeString(Path.of("static/transparencia/personal/2020-09.csv"), buffer.toString());
  }
}
