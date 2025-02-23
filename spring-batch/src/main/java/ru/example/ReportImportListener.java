package ru.example;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class ReportImportListener extends JobExecutionListenerSupport {

    private static final Log log = LogFactory.getLog(ReportImportListener.class);

    private final JdbcTemplate jdbcTemplate;

    public ReportImportListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("Отчёт загружен в базу данных");
            logAll();
        }
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Данные в таблице до загрузки отчёта");
        logAll();
    }

    private void logAll() {
        jdbcTemplate.query("SELECT region_id, outlet_id, smartphones, memory_cards, notebooks, total FROM sales_report",
                (rs, row) -> new SalesReportItem(
                        rs.getLong(1),
                        rs.getLong(2),
                        rs.getBigDecimal(3),
                        rs.getBigDecimal(4),
                        rs.getBigDecimal(5),
                        rs.getBigDecimal(6))
        ).forEach(log::info);
    }

}