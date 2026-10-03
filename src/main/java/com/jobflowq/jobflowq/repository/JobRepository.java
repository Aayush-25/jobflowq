package com.jobflowq.jobflowq.repository;

import com.jobflowq.jobflowq.model.Job;
import com.jobflowq.jobflowq.model.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

public interface JobRepository extends JpaRepository<Job, Long> {

    @Query(value = "SELECT * FROM jobs " +
            "WHERE status = 'PENDING' " +
            "ORDER BY priority DESC, created_at ASC " +
            "LIMIT 1 " +
            "FOR UPDATE SKIP LOCKED",
            nativeQuery = true)
    Optional<Job> findNextPendingJob();

    long countByStatus(JobStatus status);

    List<Job> findByStatus(JobStatus status);
}
