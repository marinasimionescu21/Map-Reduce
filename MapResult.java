import java.util.List;
import java.util.Map;

// clasa in care se retin rezultatele pentru workerii de Map
public class MapResult {
    // numele fisierului, dictionar cu lungimile cuvintelor si de cate ori apar, lista cu cele mai lungi cuvinte
    String fileName;
    Map<Integer, Integer> frequency;
    List<String> maxWords;

    public MapResult(String fileName, Map<Integer, Integer> frequency, List<String> maxWords) {
        this.fileName = fileName;
        this.frequency = frequency;
        this.maxWords = maxWords;
    }
}
