import com.sun.tools.javac.Main;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

// clasa Reduce Worker implementeaza Callable pentru a putea returna un rezultat
public class ReduceThread implements Callable {
    ArrayList<MapResult> results;

    // se primesc rezultatele obtinute dupa etapa de map
    public ReduceThread(ArrayList<MapResult> results) {
        this.results = results;
    }

    @Override
    public Object call() throws Exception {
        // se face un dictionar combinat
        Map<Integer, Integer> combined = new HashMap<>();
        int maxSize = 0;
        // se ia ultima parte din calea catre fisier, doar numele fisierului
        String filename = results.get(0).fileName.split("/")[2];

        // pentru fiecare MapResult din lista
        for (MapResult result : results) {
            // se face maximul lungimilor
            if (!result.maxWords.isEmpty() && result.maxWords.get(0).length() > maxSize) {
                maxSize = result.maxWords.get(0).length();
            }

            // pentru fiecare dictionar din result
            for (Integer key : result.frequency.keySet())
                // se verifica daca dictionarul combinat contine deja acea lungime
                // daca nu, se adauga o noua intrare cu acea lungime si cate aparitii sunt in dictionarul curent
                if (!combined.containsKey(key)) {
                    combined.put(key, result.frequency.get(key));
                } else {
                    // altfel pentru acea lungime se adauga numarul de aparitii din dictionarul curent la cele existente deja
                    combined.replace(key, combined.get(key) + result.frequency.get(key));
                }
        }

        // se calculeaza rankul dupa formula
        float rank = 0.00f;

        int nr_cuvinte = 0;
        // pentru fiecare lungime din dictionar
        for (Integer key : combined.keySet()) {
            // la rank se adauga fibo de acea cheie + 1 inmultit cu cate aparitii sunt pentru ea
            rank += Tema2.fibo.get(key + 1) * combined.get(key);
            // se calculeaza numarul total de cuvinte
            nr_cuvinte += combined.get(key);
        }
        // la final se imparte la numarul de cuvinte
        rank /= nr_cuvinte;

        // s-a calculat deja maximul lungimii
        // se creeaza lista cu toate cuvintele care au acea lungime
        ArrayList<String> maxWords = new ArrayList<>();
        for (MapResult result : results) {
            if (!result.maxWords.isEmpty() && result.maxWords.get(0).length() == maxSize) {
                maxWords.addAll(result.maxWords);
            }
        }

        // se ia numarul lor
        int noMaxLen = maxWords.size();

        // se returneaza rezultatul
        return new ReduceResult(filename, rank, maxSize, noMaxLen);
    }

}
