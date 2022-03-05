import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;

// clasa Map Worker implementeaza Callable pentru a putea returna un rezultat
public class MapThread implements Callable<MapResult> {
    private String fileName;
    private long chunkSize;
    private long position;
    private long size;

    public MapThread(String file, long chunkSize, long position, long size) {
        this.fileName = file;
        this.chunkSize = chunkSize;
        this.position = position;
        this.size = size;
    }

    @Override
    public MapResult call() {
        FileInputStream file = null;
        // delimitatorul pentru tokenizer
        String delim = ";:/?˜\\.,><‘[]{}()!@#$%ˆ&- +'=*”| \n\t\r\0";
        boolean checkBehind = false;
        boolean checkAfter = false;
        String wordAfter = "";
        try {
            // se calculeaza pana unde citeste bucata curenta
            long end = position + chunkSize;
            if (end > size)
                end = size;
            file = new FileInputStream(fileName);
            // daca suntem pe pozitia 0 nu mai verificam daca avem cuvant inainte (fragmentul nu incepe la mijlocul unui cuvant)
            if (position == 0) {
                checkBehind = false;
            } else {
                // se face skip pana la pozitia respectiva - 1
                file.skip(position - 1);
                char chr = (char) file.read();

                // se verifica daca acel caracter de dinainte sa inceapa paragraful este un delimitator
                // daca da, nu se incepe in mijlocul unui cuvant, daca nu, se incepe (si nu va fi considerat mai tarziu)
                if (!delim.contains(String.valueOf(chr)))
                    checkBehind = true;
            }

            // se citesc atatea caractere cat e dimensiunea chunk-ului sau pana la finalul fisierului
            String fragment = new String(file.readNBytes((int) (end - position)), StandardCharsets.UTF_8);
            // daca fragmentul incepe cu delimitator, nu se incepe in mijlocul unui cuvant
            if (delim.contains(String.valueOf(fragment.charAt(0))))
                checkBehind = false;

            // se sparge fragmentul in tokeni in functie de delimitatori
            ArrayList<String> tokens = new ArrayList<>();
            StringTokenizer tokenizer = new StringTokenizer(fragment, delim);
            while (tokenizer.hasMoreTokens()) {
                tokens.add(tokenizer.nextToken());
            }

            // daca am citit pana la finalul paragrafului, paragraful nu are cum sa se termine in mijlocul unui cuvant
            if (end == size) {
                checkAfter = false;
            } else {
                // alfel, se citeste caracterul de dupa, se verifica daca e delimitator, daca nu e, inseamna ca
                // paragraful e posibil sa se fi terminat in mijlocul cuvantului, dar mai trebuie o verificare
                char chr = (char) file.read();
                if (!delim.contains(String.valueOf(chr)))
                    checkAfter = true;
            }

            // daca fragmentul se termina cu delimitator atunci nu e mijlocul unui cuvant
            if (delim.contains(String.valueOf(fragment.charAt(fragment.length() - 1))))
                checkAfter = false;

            // daca avem cuvant inainte se elimina primul token, astfel ca s-a ocupat workerul precedent de acel cuvant
            if (checkBehind == true) {
                tokens.remove(0);
            }

            // daca avem cuvant care se continua in urmatorul paragraf
            if (checkAfter == true) {
                file.skip(-1);
                while (true) {
                    // se citesc caractere pana se ajunge la un delimitator sau pana la final de fisier
                    char chr = (char) file.read();
                    if (!delim.contains(String.valueOf(chr))) {
                        if (!Character.isLetterOrDigit(chr))
                            break;
                        // se adauga caracterele citite intr-o variabila
                        wordAfter = wordAfter.concat(String.valueOf(chr));
                    } else {
                        break;
                    }
                }

                // se adauga la ultimul token restul caracterului astfel incat sa se completeze cuvantul
                String token = tokens.get(tokens.size() - 1);
                tokens.remove(tokens.size() - 1);
                token = token.concat(wordAfter);
                tokens.add((token));
            }

            Map<Integer, Integer> frequency = new HashMap<>();

            // se formeaza dictionarul
            int max = -1;
            // pentru fiecare token
            for (String token : tokens) {
                // aici se retine dimensiunea maxima
                if (token.length() > max)
                    max = token.length();
                // daca nu avem deja in dictionar acea lungime, se creeaza un nou entry
                if (!frequency.containsKey(token.length())) {
                    frequency.put(token.length(), 1);
                } else {
                    // altfel se incrementeaza cel deja existents
                    frequency.replace(token.length(), frequency.get(token.length()) + 1);
                }
            }

            // dupa ce anterior am aflat lungimea maxima, se pun in lista toate cuvintele de acea dimensiune
            ArrayList<String> maxWords = new ArrayList<>();
            if (max != -1) {
                for (String token : tokens) {
                    if (token.length() == max)
                        maxWords.add(token);
                }
            }

            // se creeaza un MapResult si se returneaza
            MapResult result = new MapResult(fileName, frequency, maxWords);
            return result;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
