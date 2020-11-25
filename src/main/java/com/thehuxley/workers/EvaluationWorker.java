package com.thehuxley.workers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.thehuxley.data.RestManager;
import com.thehuxley.data.model.rest.Submission;
import com.thehuxley.queue.RabbitMQConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thehuxley.data.Configurator;
import com.thehuxley.data.JsonUtils;
import com.thehuxley.queue.RabbitMQAbstraction;

public class EvaluationWorker extends Worker {

	private static Logger logger = LoggerFactory
			.getLogger(EvaluationWorker.class);

	private int shortTime;
	private long lastTime = 0;
	private Set<Long> questIds = new HashSet<Long>();
	private final String fileName;
    private final String tempDir;

	public EvaluationWorker(int id) throws IOException {
		super(id);
		shortTime = Integer.parseInt(Configurator.getProperties(
				Constants.CONF_FILENAME).getProperty(
				"worker.evaluation.short_time"));

		tempDir = Configurator.getProperties(Constants.CONF_FILENAME)
                .getProperty("worker.evaluation.temp.dir");

        fileName = tempDir+ Configurator.getProperties(Constants.CONF_FILENAME)
				.getProperty("worker.evaluation.queue_ids_filename")+"_"+id;

		Set<Long> fromFile = loadSet();
		if (fromFile != null) {
			questIds = fromFile;
			updateQuestionnaireStatistics();
		}
	}

	@Override
	public boolean onMessage(String message) {
		long interval = 0;
		if (lastTime!=0){
			interval = System.currentTimeMillis() - lastTime;
		}
		

		Submission submission = JsonUtils.fromJson(message, Submission.class);
        if (logger.isInfoEnabled()){
            logger.info("Received submission: "+submission.getId());
        }

		List<Long> questIdList = RestManager
				.triggerAfterSubmissionEvaluation(submission);

		if (questIdList != null) {
			questIds.addAll(questIdList);
		}

		// Salva em disco, para o caso em que o worker é parado por algum motivo
		saveSet(questIds);

		if (interval > shortTime) {
			updateQuestionnaireStatistics();
		}

		lastTime = System.currentTimeMillis();
		return true;
	}

	public void updateQuestionnaireStatistics() {
		for (Long questId : questIds) {
			if (logger.isInfoEnabled()){
				logger.info("updateQuestionnaireStatistics. questId="+questId);
			}
			DataManager.updateQuestionnaireStatistics(questId);
		}
		questIds.clear();
		// apaga do disco
		deleteFile();
	}

	@Override
	protected RabbitMQConsumer createRabbitMQAConsumer()
			throws IOException {
		Properties prop = Configurator.getProperties(Constants.CONF_FILENAME);
		return new RabbitMQConsumer(
				prop.getProperty("rabbitmq.evaluation.queueName"),
				prop.getProperty("rabbitmq.evaluation.hostName"),
				Integer.parseInt(prop
						.getProperty("rabbitmq.evaluation.serverdown.sleep")));
	}

	private void deleteFile() {
		File f = new File(fileName);
		if (!f.delete()) {
			logger.error("File :" + fileName + " could not be deleted");
		}
	}

	private Set<Long> loadSet() {
		BufferedReader br = null;
		HashSet<Long> result = null;
		try {
			br = new BufferedReader(new FileReader(fileName));
			String line;
			while ((line = br.readLine()) != null) {
				if (result == null) {
					result = new HashSet<Long>();
				}
				result.add(Long.parseLong(line));
			}
		}catch (FileNotFoundException e) {
			return result;
		}catch (IOException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return result;

	}

	private void saveSet(Set<Long> questIds) {
		PrintWriter out = null;
		try {
			out = new PrintWriter(new FileOutputStream(fileName));
			for (Long id : questIds) {
				out.println(id.toString());
			}
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

}
