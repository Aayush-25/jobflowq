package com.jobflowq.jobflowq.dto;

public class QueueMetrics {

    private long pending;
    private long processing;
    private long completed;
    private long failed;
    private long dead;
    private long totalProcessed;

    public QueueMetrics(long pending, long processing, long completed,
                        long failed, long dead, long totalProcessed) {
        this.pending = pending;
        this.processing = processing;
        this.completed = completed;
        this.failed = failed;
        this.dead = dead;
        this.totalProcessed = totalProcessed;
    }

    public long getPending() { return pending; }
    public long getProcessing() { return processing; }
    public long getCompleted() { return completed; }
    public long getFailed() { return failed; }
    public long getDead() { return dead; }
    public long getTotalProcessed() { return totalProcessed; }
}
