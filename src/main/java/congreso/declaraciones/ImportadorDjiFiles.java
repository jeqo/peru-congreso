package congreso.declaraciones;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import org.bson.internal.Base64;
import org.jsoup.Jsoup;

public class ImportadorDjiFiles {

  public static void main(String[] args) throws IOException {
    var lines = Files.readAllLines(Path.of("static/congreso/dji/2020.csv"))
        .stream().dropWhile(s -> s.startsWith("ENTIDAD"))
        .collect(Collectors.toList());
    lines.stream().parallel().forEach(l -> {
      try {
        var c = l.split(",");
        var urlText = c[c.length - 1];
        if (!urlText.equals("null")) {
          System.out.println(urlText);
          var doc = Jsoup.connect(urlText).get();
          final var data = doc.select("object").first().attr("data");
          Files.write(Path.of("static/congreso/dji/2020/" + c[2].replace(" ", "_") + ".pdf"),
              Base64.decode(data.split(";")[1].split(",")[1]));
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }
}
