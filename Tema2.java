import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Tema2 {
    public static ConcurrentHashMap<String, String> hashMap;
    public static ArrayList<Integer> fibo = new ArrayList<>();

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }

        // valorile initiale din lista cu fibonacii
        fibo.addAll(Arrays.asList(0,1,1,2));

        // parametrii din linie de comanda
        Integer noWorkers = Integer.parseInt(args[0]);
        String pathInput = args[1];
        String pathOutput = args[2];
        FileReader input = null;
        try {
            // Se citeste numarul de fisiere, dimensiunea chunkului si numele fisierelor
            input = new FileReader(pathInput);
            BufferedReader inputReader = new BufferedReader(input);
            Integer chunkSize = Integer.parseInt(inputReader.readLine());
            Integer noFiles = Integer.parseInt(inputReader.readLine());

            ArrayList<String> files = new ArrayList<>();

            for (int i = 0; i < noFiles; i++) {
                files.add(inputReader.readLine());
            }

            // se preiau lungimile tuturor fisierelor
            BufferedReader reader;
            ArrayList<Long> filesSizes = new ArrayList<>();
            for (int i = 0; i < noFiles; i++) {
                File file = new File(files.get(i));
                filesSizes.add(file.length());
            }

            // se declara un ExecutorService cu un thread pool fix (nr workeri primit ca parametru)
            MapThread thread;
            ExecutorService executorService = Executors.newFixedThreadPool(noWorkers);
            int maxLen = -1;
            ArrayList<Long> sizes = new ArrayList<>();
            List<MapResult> results = new ArrayList<>();

            // pentru fiecare fisier
            for (int i = 0; i < noFiles; i++) {
                // se calculeaza in cate bucati se imparte in functie de dimensiunea chunckului
                long size;
                size = filesSizes.get(i) / chunkSize;

                if (filesSizes.get(i) % chunkSize != 0)
                    size += 1;
                sizes.add(size);

                // Se declara o lista de futures unde se vor astepta rezultatele returnate de workerii Map
                List<Future> futures = new ArrayList<>();
                for (int j = 0; j < size; j++) {
                    // Se face submit la fiecare bucata din fisier, iar rezultatul se pune intr-o lista de futures
                    Future<MapResult> res = executorService.submit(new MapThread(files.get(i), chunkSize, chunkSize * j, filesSizes.get(i)));
                    futures.add(res);
                }


                for (Future<MapResult> future : futures) {
                    // Se preiau informatiile obtinute folosing metoda get
                    // Metoda get este blocanta, adica nu se va executa cod dupa instructiunea de get, decat atunci cand
                    // taskul isi va fi terminat operatia de Map si a intors rezultatul
                    MapResult result = future.get();
                    results.add(result);
                    // Se calculeaza cel mai lung cuvant in timp ce se extrag rezultatele pentru a sti exact cate numere
                    // din sirul lui fibonacci sa se calculeze
                    if (!result.maxWords.isEmpty() && result.maxWords.get(0).length() > maxLen)
                        maxLen = result.maxWords.get(0).length();
                }

            }
            // avand cel mai lung cuvant din setul de fisiere, se vor calcula atatea numere + 1 din sirul lui fibonacci
            // astfel, avem deja calculate numerele, nu trebuie recalculate de fiecare data
            // Acestea sunt retinute intr-o variabila statica partajata, la care au acces toti workerii
            for (int j = 4; j < maxLen + 2; j++) {
                fibo.add(fibo.get(j - 1) + fibo.get(j - 2));
            }
            int k = 0;

            // Dupa ce ne-am asigurat ca s-au terminat operatiile de Map (nu se poate ajunge aici decat dupa ce s-a
            // extras si ultimul rezultat de Map din future), vom incepe operatiile de reduce
            List<Future> futures = new ArrayList<>();
            // am retinut in timp ce faceam map cate bucati are fiecare fisier, asadar vom grupa rezultatele calculate
            // anterior in liste de tio MapResult, in functie de cate are fiecare fisier
            for (int i = 0; i < noFiles; i++) {
                ArrayList<MapResult> res = new ArrayList<>();
                // pentru fiecare fisier, trimitem toate rezultatele obtinute pentru acel fisier
                for (int j = 0; j < sizes.get(i); j++, k++) {
                    res.add(results.get(k));
                }
                // se face submit la task-uri si se pastreaza rezultatul, la fel, intr-un future
                Future<ReduceResult> rez = executorService.submit(new ReduceThread(res));
                futures.add(rez);
            }


            // Se extrag rezultatele intoarse de workerii de Reduce
            ArrayList<ReduceResult> reduceResults = new ArrayList<>();
            for (Future<ReduceResult> future : futures) {
                ReduceResult result = future.get();
                reduceResults.add(result);
            }
            // se opreste executorul
            executorService.shutdown();

            // se sorteeaza rezultatele in functie de rank

            Collections.sort(reduceResults, (ReduceResult o1, ReduceResult o2) -> o1.getRank() > o2.getRank() ? -1 : 1);

            // se scriu rezultatele la in fisierul de output
            FileWriter writer = new FileWriter(pathOutput);
            for (ReduceResult reduceResult : reduceResults) {
                String res = reduceResult.toString();
                writer.write(res + "\n");
            }
            // se inchide fisierul
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
