public class StatRun {
    public StatRun(int batchSize, int qos, String type, double value) {
        this.batchSize = batchSize;
        this.qos = qos;
        this.type = type;
        this.value = value;
    }

    private final int batchSize;
    private final int qos;
    private final String type;
    private final double value;

    public int getBatchSize() {
        return batchSize;
    }

    public int getQos() {
        return qos;
    }

    public String getType() {
        return type;
    }

    public double getValue() {
        return value;
    }
}