// clasa pentru rezultatele returnate de workerii Reduce
public class ReduceResult {
    String fileName;
    float rank;
    int maxSize;
    int noMaxSize;

    public ReduceResult(String fileName, float rank, int maxSize, int noMaxSize) {
        this.fileName = fileName;
        this.rank = rank;
        this.maxSize = maxSize;
        this.noMaxSize = noMaxSize;
    }

    // Getters ans Setters
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public float getRank() {
        return rank;
    }

    public void setRank(float rank) {
        this.rank = rank;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public int getNoMaxSize() {
        return noMaxSize;
    }

    public void setNoMaxSize(int noMaxSize) {
        this.noMaxSize = noMaxSize;
    }

    // Override la metoda toString
    @Override
    public String toString() {
        return fileName + "," + String.format("%.2f", rank) + "," + maxSize + "," + noMaxSize;
    }
}
