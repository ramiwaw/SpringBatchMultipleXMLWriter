package com.example.springbatchmultiplexmlwriter.config;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.MultiResourceItemWriter;
import org.springframework.batch.item.file.ResourceSuffixCreator;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.example.springbatchmultiplexmlwriter.model.Comment;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	/*
	 * DataSource : MySQL v5.1.47
	 */
	@Bean
	public DataSource dataSource() {

		DriverManagerDataSource dataSource = new DriverManagerDataSource();

		dataSource.setDriverClassName("com.mysql.jdbc.Driver");
		dataSource.setUrl("jdbc:mysql://localhost/testdb?useSSL=false");
		dataSource.setUsername("root");
		dataSource.setPassword("YoYo@2!12");

		return dataSource;
	}

	/*
	 * JdbcCursorItemReader<T> : Simple item reader implementation that opens a
	 * JDBC cursor and continually retrieves the next row in the ResultSet.
	 */
	@Bean
	public JdbcCursorItemReader<Comment> reader() {

		JdbcCursorItemReader<Comment> reader = new JdbcCursorItemReader<>();

		reader.setDataSource(dataSource());
		reader.setSql("SELECT id, content FROM comment");
		reader.setRowMapper(new CommentRowMapper());

		return reader;
	}

	/*
	 * RowMapper<T> : An interface used by JdbcTemplate for mapping rows of a
	 * ResultSet on a per-row basis.
	 */
	public class CommentRowMapper implements RowMapper<Comment> {

		@Override
		public Comment mapRow(ResultSet rs, int rowNum) throws SQLException {

			Comment comment = new Comment();

			comment.setId(rs.getInt("id"));
			comment.setContent(rs.getString("content"));

			return comment;
		}

	}

	@Bean
	public CommentItemProcessor processor() {
		return new CommentItemProcessor();
	}

	/*
	 * Interface ItemProcessor<I,O> : Interface for item transformation.
	 */
	public class CommentItemProcessor implements ItemProcessor<Comment, Comment> {

		@Override
		public Comment process(Comment comment) throws Exception {

			return comment;
		}

	}

	/*
	 * FlatFileItemWriter<T> : This class is an item writer that writes data to
	 * a file or stream.
	 */
	@Bean
	public StaxEventItemWriter<Comment> writer() {

		StaxEventItemWriter<Comment> xmlFileWriter = new StaxEventItemWriter<>();

		xmlFileWriter.setRootTagName("comments");

		
		
		Jaxb2Marshaller empMarshaller = new Jaxb2Marshaller();
		empMarshaller.setClassesToBeBound(Comment.class);
		xmlFileWriter.setMarshaller(empMarshaller);
		xmlFileWriter.setOverwriteOutput(false);
		xmlFileWriter.setShouldDeleteIfEmpty(true);
		xmlFileWriter.setEncoding(StandardCharsets.UTF_8.toString());
		
		//String exportFilePath = "F:\\test\\aaa";
		//xmlFileWriter.setResource(new FileSystemResource(exportFilePath));

		return xmlFileWriter;

	}

	@Bean
	public MultiResourceItemWriter<Comment> multiResourceItemWriter(StaxEventItemWriter<Comment> itemWriter) {

		MultiResourceItemWriter<Comment> multiResourceItemWriter = new MultiResourceItemWriter<>();
		

		multiResourceItemWriter.setItemCountLimitPerResource(1);
		String exportFilePath = "F:/test/aaa";
		multiResourceItemWriter.setResource(new FileSystemResource(exportFilePath));
		//multiResourceItemWriter.setResourceSuffixCreator(getSuffix());
		multiResourceItemWriter.setDelegate(itemWriter);
		

		return multiResourceItemWriter;
	}

	@Bean
	public ResourceSuffixCreator getSuffix() {
		return new ResourceSuffixCreator() {

			@Override
			public String getSuffix(int index) {
				// TODO Auto-generated method stub
				return "XXX_" + index + ".xml";
			}

		};
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<Comment, Comment>chunk(1).reader(reader()).processor(processor())
				.writer(multiResourceItemWriter(writer())).build();
				//.writer(writer()).build();
	}

	@Bean
	public Job exportUserJob() {
		return jobBuilderFactory.get("job1").incrementer(new RunIdIncrementer()).flow(step1()).end().build();
	}
}
