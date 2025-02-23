package ru.example.config;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.example.IncorrectValueException;
import ru.example.ReportImportListener;
import ru.example.SalesReportItem;

import javax.sql.DataSource;
import java.math.BigDecimal;

//@EnableBatchProcessing ВАЖНА АННОТАЦИЯ.
// аннотация, активирующая многие функции экосистемы Spring Batch,
// в частности конфигурирует фабрики JobBuilderFactory и
// StepBuilderFactory, создает реестр заданий и многие другие
// инфраструктурные элементы.
@Configuration
@EnableBatchProcessing
public class BatchConfig {
    private static final Log log = LogFactory.getLog(BatchConfig.class);

    // Создание тасклета, т.е. произвольного кода,
    // который будет выполнен на первом этапе загрузки отчёта.
    // Этот код, как видно, удаляет все существующие данные в таблице отчёта.
    @Bean
    public Tasklet clearTableTasklet(JdbcTemplate jdbcTemplate) {
        return (stepContribution, chunkContext) -> {
            log.info("Очистка таблицы sales_report");
            jdbcTemplate.update("delete from sales_report");
            return RepeatStatus.FINISHED;
        };
    }
    @Bean // Создание источника данных (DataSource) для подключения к Postgres.
    public DataSource dataSource() {
        return DataSourceBuilder.create()
                .driverClassName("org.postgresql.Driver")
                .url("jdbc:postgresql://localhost/springbatch")
                .username("postgres")
                .password("root")
                .build();
    }

    @Bean // Создание jdbcTemplate с использованием нового dataSource.
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    public ReportImportListener reportImportListener(DataSource dataSource) {
        return new ReportImportListener(new JdbcTemplate(dataSource));
    }


    @Bean //Создание первого шага обработки отчёта на основе тасклета.
    public Step setupStep(Tasklet clearTableTasklet,
                          StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("clear-report-table")
                .tasklet(clearTableTasklet)
                .build();
    }

    @Bean // Создание второго шага, непосредственной загрузки данных отчёта из файла
    public Step loadCsvStep(StepBuilderFactory stepBuilderFactory,
                            FlatFileItemReader<SalesReportItem> csvReader,
                            ItemProcessor<SalesReportItem, SalesReportItem> totalCalculatingProcessor,
                            JdbcBatchItemWriter<SalesReportItem> dbWriter) {
        return stepBuilderFactory.get("load-csv-file")
                .<SalesReportItem, SalesReportItem>chunk(10) // Здесь задаётся размер порции данных
                .faultTolerant()
                .skip(IncorrectValueException.class) // Это относится к валидации строк отчёта
                .skipLimit(3) // Это относится к валидации строк отчёта
                .reader(csvReader) //
                .processor(totalCalculatingProcessor) //
                .writer(dbWriter) //
                .build();
    }

    @Bean
    @StepScope//считыватель
    public FlatFileItemReader<SalesReportItem> csvReader() {
        return new FlatFileItemReaderBuilder<SalesReportItem>()
                .name("csv-reader")
                .resource(new ClassPathResource("report_data.csv"))
                .targetType(SalesReportItem.class)
                .delimited()
                .delimiter("\t") // Используйте табуляцию как разделитель
                .names(new String[]{"regionId", "outletId", "smartphones", "memoryCards", "notebooks"})
                .build();
    }

    @Bean // Процессор данных, реализующий функциональный интерфейс ItemProcessor
    public ItemProcessor<SalesReportItem, SalesReportItem> totalCalculatingProcessor() {
        return item -> {//просто для примера проверяем, что сумма выручки не является отрицательным числом
            if (BigDecimal.ZERO.compareTo(item.getSmartphones()) > 0
                    || BigDecimal.ZERO.compareTo(item.getMemoryCards()) > 0
                    || BigDecimal.ZERO.compareTo(item.getNotebooks()) > 0) {
                throw new IncorrectValueException();
            }
            item.setTotal(BigDecimal.ZERO.add(item.getSmartphones())
                    .add(item.getMemoryCards()
                            .add(item.getNotebooks())));
            return item;
        };
    }

    @Bean // Записыватель данных, который отображает каждый объект POJO в строку в базе данных.
    public JdbcBatchItemWriter<SalesReportItem> dbWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<SalesReportItem>()
                .dataSource(dataSource)
                .sql("insert into sales_report (region_id, outlet_id, smartphones, memory_cards, notebooks, total) " +
                        "values (:regionId, :outletId, :smartphones, :memoryCards, :notebooks, :total)")
                .beanMapped()
                .build();
    }

    @Bean // Создание объекта задания (Job), состоящего из последовательности двух шагов.
    public Job importReportJob(JobBuilderFactory jobBuilderFactory,
                               Step setupStep,
                               Step loadCsvStep,
                               ReportImportListener reportImportListener) {
        return jobBuilderFactory.get("import-report-job")
                .incrementer(new RunIdIncrementer())
                 .listener(reportImportListener) // Слушатель, отслеживающий этапы жизненного цикла задания.
                .start(setupStep)
                .next(loadCsvStep)
                .build();
    }

}